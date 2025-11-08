# ============================================
# Greater Sydney+ Accessibility (MB → TZ jobs)
# Manual chunks (save per-chunk) + Stitch mode
# Now with run_mode = "all" to process every chunk
# ============================================

rm(list = ls()); gc()

# -------- CONFIG --------
options(java.parameters = "-Xmx12g")  # adjust to your RAM

# Paths
data_path       <- 
tz_data_dir     <- 
mb_data_dir     <- 
mb_centroids_csv<- file.path(mb_data_dir, "MB_CODE_2021_CENTROID.csv")
mb_shp_path     <- file.path(mb_data_dir, "MB_2021_AUST_GDA2020.shp")
out_dir         <- 

# Run mode: "chunk" (one chunk), "all" (every chunk), "stitch" (merge & map)
run_mode        <- "chunk"       # <- change to "all" or "stitch" as needed
chunk_size      <- 500L          # how many origins per chunk
chunk_id        <- 1L            # which chunk to run when run_mode = "chunk"
overwrite_chunks<- FALSE         # if TRUE, recompute even if file exists

# BBox (fast numeric filter) — includes Bondi
lon_min <- 150.5670   # West: Springwood (use 150.6000 if you prefer Blaxland)
lon_max <- 151.3500   # East: safely past Bondi/Watsons Bay (~151.27–151.30)
lat_min <- -34.2000   # South: unchanged
lat_max <- -33.5485   # North: Brooklyn

# Thinning (to keep runs stable)
snap_step_deg <- 0.0003   # ~30 m de-dup; increase to 0.001 (~100 m) if still too many
grid_thin_m   <- 0        # 0=off; set 500 for ~500 m thinning (keeps 1 / 500m cell)

# Routing controls
departure_time <- as.POSIXct("2025-09-09 08:00:00", tz = "Australia/Sydney")
modes          <- c("WALK","BUS","RAIL","SUBWAY","TRAM","FERRY")  # bus-only: c("BUS","WALK")
cutoff_min     <- 45
max_trip_min   <- 120   

# ----- TIME-SAMPLING CONTROLS (every 10 min from 07:55–08:55, inclusive) -----
time_mode      <- "grid"        # "single" | "grid"
time_start_str <- "2025-09-09 08:00:00"
time_end_str   <- "2025-09-09 08:01:00"
time_step_min  <- 1            # → 07:55, 08:05, 08:15, 08:25, 08:35, 08:45, 08:55 (7 samples)
time_agg_fun   <- "mean"        # or "median" for robust smoothing

# Build vector of departure times
time_start <- as.POSIXct(time_start_str, tz = "Australia/Sydney")
time_end   <- as.POSIXct(time_end_str,   tz = "Australia/Sydney")
build_dep_times <- function(mode = "grid") {
  if (mode == "single") {
    return(departure_time)
  } else if (mode == "grid") {
    return(seq(time_start, time_end, by = paste0(time_step_min, " mins")))  # inclusive when aligned
  } else stop("time_mode must be 'single' or 'grid'")
}
dep_times <- build_dep_times(time_mode)
message("Sampling ", length(dep_times), " departure time(s): ",
        paste(format(dep_times, "%H:%M"), collapse = ", "))

# ------------------------
# Libraries & Java init
library(rJava); .jinit(); options(java.home = NULL); Sys.unsetenv("JAVA_HOME")
library(r5r)
library(sf)
library(dplyr)
library(readr)
library(leaflet)
library(data.table)
library(stringr)

dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

# ---------------------------
# Build / Load R5 core
# ---------------------------
print(list.files(data_path, pattern = "\\.(pbf|zip)$", full.names = TRUE))
r5r_core <- setup_r5(
  data_path = data_path,
  elevation = "TOBLER",
  verbose   = TRUE,
  overwrite = FALSE
)
stopifnot(!is.null(r5r_core))
# Use most cores but keep one free for the OS (tweak if needed)
r5r::set_number_of_threads(r5r_core, n_threads = max(1, parallel::detectCores() - 1))

# ---------------------------
# Study bbox polygon (for precise clip later)
# ---------------------------
gs_bbox_sf <- st_as_sfc(st_bbox(c(xmin = lon_min, xmax = lon_max, ymin = lat_min, ymax = lat_max), crs = 4326))

# ---------------------------
# Verify GTFS Dates
# ---------------------------
gtfs_zips <- list.files(data_path, pattern = "\\.zip$", full.names = TRUE)

get_range <- function(z) {
  if (!any(utils::unzip(z, list = TRUE)$Name %in% c("calendar.txt","calendar_dates.txt"))) return(NULL)
  rng <- NULL
  if ("calendar.txt" %in% utils::unzip(z, list = TRUE)$Name) {
    cal <- readr::read_csv(unz(z, "calendar.txt"), show_col_types = FALSE,
                           col_types = readr::cols(.default = readr::col_guess(),
                                                   start_date = readr::col_character(),
                                                   end_date   = readr::col_character()))
    if (nrow(cal)) {
      sd <- min(as.Date(cal$start_date, format = "%Y%m%d"), na.rm = TRUE)
      ed <- max(as.Date(cal$end_date,   format = "%Y%m%d"), na.rm = TRUE)
      rng <- c(sd, ed)
    }
  }
  if ("calendar_dates.txt" %in% utils::unzip(z, list = TRUE)$Name) {
    cdat <- readr::read_csv(unz(z, "calendar_dates.txt"), show_col_types = FALSE,
                            col_types = readr::cols(.default = readr::col_guess(),
                                                    date = readr::col_character()))
    if (nrow(cdat)) {
      dmin <- min(as.Date(cdat$date, format = "%Y%m%d"), na.rm = TRUE)
      dmax <- max(as.Date(cdat$date, format = "%Y%m%d"), na.rm = TRUE)
      rng <- if (is.null(rng)) c(dmin, dmax) else c(min(rng[1], dmin), max(rng[2], dmax))
    }
  }
  if (is.null(rng)) NULL else data.frame(zip = basename(z), start = rng[1], end = rng[2])
}

ranges <- do.call(rbind, lapply(gtfs_zips, get_range))
print(ranges)
cat("Chosen departure date:", as.Date(departure_time), "\n")
if (nrow(ranges)) {
  covered <- any(as.Date(departure_time) >= ranges$start & as.Date(departure_time) <= ranges$end)
  cat("Covered by at least one GTFS? ", covered, "\n")
}

# ---------------------------
# Destinations: TZ jobs → centroids
# ---------------------------
employment_csv  <- file.path(tz_data_dir, "tzp24-employment-by-tz-by-industry-2021-2066.csv")
stopifnot(file.exists(employment_csv))
employment_data <- read_csv(employment_csv, show_col_types = FALSE)

employment_agg  <- employment_data %>%
  group_by(TZ_CODE21) %>%
  summarise(total_EMP_2021 = sum(EMP_2021, na.rm = TRUE), .groups = "drop") %>%
  mutate(TZ_CODE21 = as.character(TZ_CODE21))

tz_geojson      <- file.path(tz_data_dir, "TZ21_TfNSW_geojson.json")
stopifnot(file.exists(tz_geojson))
travel_zones    <- st_read(tz_geojson, quiet = TRUE)

travel_zones_emp <- travel_zones %>%
  mutate(tz21_code = as.character(tz21_code)) %>%
  left_join(employment_agg, by = c("tz21_code" = "TZ_CODE21"))

# clip to bbox
travel_zones_emp_gs <- travel_zones_emp[st_intersects(travel_zones_emp, gs_bbox_sf, sparse = FALSE), ]

# centroids with jobs
destinations <- travel_zones_emp_gs %>%
  st_centroid() %>%
  mutate(
    lon = st_coordinates(.)[,1],
    lat = st_coordinates(.)[,2],
    id  = tz21_code
  ) %>%
  st_drop_geometry() %>%
  select(id, lon, lat, total_EMP_2021) %>%
  filter(is.finite(lon), is.finite(lat), !is.na(total_EMP_2021))

message("Destinations (TZs): ", nrow(destinations))

# ---------------------------
# Origins: MB centroids → filters + de-dup + (optional) thinning
# ---------------------------
stopifnot(file.exists(mb_centroids_csv))
origins_raw <- read_csv(mb_centroids_csv, show_col_types = FALSE)

# harmonise common ArcGIS headers
if ("MB_CODE21" %in% names(origins_raw) && !("MB_CODE_2021" %in% names(origins_raw))) {
  origins_raw <- origins_raw %>% rename(MB_CODE_2021 = MB_CODE21)
}
if ("POINT_X" %in% names(origins_raw) && !("lon" %in% names(origins_raw))) {
  origins_raw <- origins_raw %>% rename(lon = POINT_X)
}
if ("POINT_Y" %in% names(origins_raw) && !("lat" %in% names(origins_raw))) {
  origins_raw <- origins_raw %>% rename(lat = POINT_Y)
}
stopifnot(all(c("MB_CODE_2021","lon","lat") %in% names(origins_raw)))

origins <- origins_raw %>%
  transmute(
    id         = as.character(MB_CODE_2021),
    lon        = as.numeric(lon),
    lat        = as.numeric(lat),
    population = dplyr::coalesce(as.numeric(Person), 0)
  )

message("MB origins raw: ", nrow(origins))

# fast numeric bbox (KEEP pop==0 too)
origins <- origins %>%
  filter(
    is.finite(lon), is.finite(lat),
    lon >= lon_min, lon <= lon_max,
    lat >= lat_min, lat <= lat_max
  )
message("After bbox (pop kept incl. 0): ", nrow(origins))

# de-duplicate by snapping to small grid (e.g., ~30 m)
snap <- function(x, step) step * round(x / step)
origins <- origins %>%
  mutate(
    lon_s = snap(lon, snap_step_deg),
    lat_s = snap(lat, snap_step_deg)
  ) %>%
  distinct(lon_s, lat_s, .keep_all = TRUE) %>%
  select(-lon_s, -lat_s)

message("After ~", round(snap_step_deg * 111000), "m de-dup: ", nrow(origins))

# optional: spatial grid thinning (keeps most populated in each cell)
if (grid_thin_m > 0) {
  message("Applying grid thinning at ~", grid_thin_m, " m ...")
  orig_sf <- st_as_sf(origins, coords = c("lon","lat"), crs = 4326) |> st_transform(3857)
  grid    <- st_make_grid(orig_sf, cellsize = grid_thin_m)
  idx     <- st_intersects(orig_sf, grid)
  orig_sf$cell <- vapply(idx, function(x) if(length(x)) x[1] else NA_integer_, integer(1))
  origins <- orig_sf %>%
    filter(!is.na(cell)) %>%
    group_by(cell) %>%
    slice_max(population, n = 1, with_ties = FALSE) %>%
    ungroup() %>%
    mutate(lon = st_coordinates(geometry)[,1],
           lat = st_coordinates(geometry)[,2]) %>%
    st_drop_geometry() %>%
    select(id, lon, lat, population)
  message("After grid thinning: ", nrow(origins))
}

# (belt & braces) precise sf clip to bbox
origins_sf <- st_as_sf(origins, coords = c("lon","lat"), crs = 4326)
origins_sf <- origins_sf[st_intersects(origins_sf, gs_bbox_sf, sparse = FALSE), ]
origins    <- origins_sf %>%
  mutate(lon = st_coordinates(geometry)[,1],
         lat = st_coordinates(geometry)[,2]) %>%
  st_drop_geometry()

# Stable ordering so chunk boundaries stay consistent
origins <- origins %>% arrange(id) %>% as.data.frame()

message("Origins ready for routing: ", nrow(origins))


# ===========================
# helpers
# ===========================
.aggregate_time <- function(df_list, fun = "mean") {
  acc_all <- dplyr::bind_rows(df_list)
  fun_f <- switch(fun,
                  "mean"   = ~mean(.x, na.rm = TRUE),
                  "median" = ~median(.x, na.rm = TRUE),
                  stop("Unsupported time_agg_fun"))
  acc_all %>%
    dplyr::group_by(id) %>%
    dplyr::summarise(accessibility = fun_f(accessibility), .groups = "drop")
}

process_chunk <- function(idx, origins_df, dep_times, out_dir, overwrite = FALSE) {
  n_orig   <- nrow(origins_df)
  chunk_sz <- chunk_size
  n_chunks <- ceiling(n_orig / chunk_sz)
  if (idx < 1L || idx > n_chunks) {
    stop(sprintf("chunk_id=%d out of range (1..%d). n_origins=%d, chunk_size=%d",
                 idx, n_chunks, n_orig, chunk_sz))
  }
  i_start <- (idx - 1L) * chunk_sz + 1L
  i_end   <- min(idx * chunk_sz, n_orig)
  ii      <- i_start:i_end
  
  message(sprintf("Running chunk %d/%d (origins %d..%d of %d)",
                  idx, n_chunks, i_start, i_end, n_orig))
  
  out_path <- file.path(out_dir, sprintf("acc_chunk_%04d.csv.gz", idx))
  if (file.exists(out_path) && !overwrite) {
    message("Skipping (exists): ", out_path)
    return(invisible(out_path))
  }
  
  acc_list <- vector("list", length(dep_times))
  for (k in seq_along(dep_times)) {
    dt_k <- dep_times[k]
    message("  - time ", format(dt_k))
    acc_list[[k]] <- r5r::accessibility(
      r5r_core               = r5r_core,
      origins                = origins_df[ii, ],
      destinations           = destinations,
      opportunities_colnames = "total_EMP_2021",
      mode                   = modes,
      departure_datetime     = dt_k,
      decay_function         = "step",
      cutoffs                = cutoff_min,
      max_trip_duration      = max_trip_min,
      verbose                = TRUE
    ) %>% as.data.frame()
  }
  
  acc_k <- .aggregate_time(acc_list, fun = time_agg_fun)
  readr::write_csv(acc_k, out_path)
  message("Saved: ", out_path)
  invisible(out_path)
}

# ===========================
# MODE A: MANUAL CHUNK RUN
# ===========================
if (run_mode == "chunk") {
  process_chunk(chunk_id, origins, dep_times, out_dir, overwrite = overwrite_chunks)
}

# ===========================
# MODE A2: PROCESS ALL CHUNKS
# ===========================
if (run_mode == "all") {
  n_orig   <- nrow(origins)
  n_chunks <- ceiling(n_orig / chunk_size)
  message("Processing ALL chunks: ", n_chunks)
  for (idx in seq_len(n_chunks)) {
    try({
      process_chunk(idx, origins, dep_times, out_dir, overwrite = overwrite_chunks)
    }, silent = FALSE)
  }
  message("All chunks processed.")
}

# ===========================
# MODE B: STITCH & MAP (MB POLYGONS if available, else points)
# ===========================
if (run_mode == "stitch") {
  files <- list.files(out_dir, pattern = "^acc_chunk_\\d{4}\\.csv\\.gz$", full.names = TRUE)
  if (!length(files)) stop("No chunk files found in out_dir")
  message("Found ", length(files), " chunk files")
  
  # Stitch accessibility (id = origin MB code)
  accessibility_df <- files %>%
    lapply(readr::read_csv, show_col_types = FALSE) %>%
    dplyr::bind_rows() %>%
    dplyr::mutate(id = as.character(id)) %>%
    dplyr::rename(MB_CODE_2021 = id)
  
  message("Combined accessibility rows: ", nrow(accessibility_df))
  
  # Complete origin table (ALL MBs that were routed, incl. pop==0)
  all_origins <- origins %>% dplyr::transmute(MB_CODE_2021 = id, lon, lat, population)
  
  # Complete accessibility for ALL MBs: NA -> 0
  acc_complete <- all_origins %>%
    dplyr::left_join(accessibility_df, by = "MB_CODE_2021") %>%
    dplyr::mutate(accessibility = dplyr::coalesce(accessibility, 0))
  
  # Legend bins (fine steps up to 1,000,000+)
  bins <- c(
    0, 1000, 2500, 5000, 7500, 10000,
    25000, 50000, 75000, 100000,
    250000, 500000, 750000, 1000000, Inf
  )
  lab_fmt <- function(type, cuts, p) {
    cf <- format(cuts, big.mark = ",", scientific = FALSE, trim = TRUE)
    n  <- length(cuts)
    c(paste0(cf[1:(n-2)], " – ", cf[2:(n-1)]),
      paste0(cf[n-1], " +"))
  }
  
  # ---------- Try polygons first ----------
  have_mb_polys <- file.exists(mb_shp_path)
  
  if (have_mb_polys) {
    # bbox-filtered read; fallback to read+crop
    wkt_box <- sf::st_as_text(gs_bbox_sf)
    mb_polys <- tryCatch(
      sf::st_read(mb_shp_path, quiet = TRUE, wkt_filter = wkt_box),
      error = function(e) { message("wkt_filter not supported; reading then cropping…"); NULL }
    )
    if (is.null(mb_polys)) {
      mb_polys <- sf::st_read(mb_shp_path, quiet = TRUE) %>% sf::st_crop(gs_bbox_sf)
    }
    
    if (!("MB_CODE_2021" %in% names(mb_polys)) && "MB_CODE21" %in% names(mb_polys)) {
      mb_polys <- dplyr::rename(mb_polys, MB_CODE_2021 = MB_CODE21)
    }
    mb_polys <- mb_polys %>%
      dplyr::mutate(MB_CODE_2021 = as.character(MB_CODE_2021)) %>%
      sf::st_transform(4326) %>%
      dplyr::mutate(.cent = sf::st_centroid(geometry),
                    .lat  = sf::st_coordinates(.cent)[,2]) %>%
      dplyr::filter(.lat >= lat_min, .lat <= lat_max) %>%
      dplyr::select(-.cent, -.lat)
    
    mb_map <- mb_polys %>% dplyr::left_join(acc_complete, by = "MB_CODE_2021")
    
    mb_map_pop0  <- mb_map %>% dplyr::filter(population == 0)
    mb_map_popgt <- mb_map %>% dplyr::filter(population > 0)
    
    pal <- leaflet::colorBin(
      palette = "YlOrRd",
      domain  = c(mb_map_popgt$accessibility, mb_map_pop0$accessibility),
      bins    = bins,
      right   = FALSE
    )
    
    m <- leaflet::leaflet() %>%
      leaflet::addTiles() %>%
      leaflet::addPolygons(
        data        = mb_map_popgt,
        fillColor   = ~pal(accessibility),
        weight      = 0.2, color = "#666",
        opacity     = 1,   fillOpacity = 0.7,
        popup       = ~paste0(
          "<b>MB: </b>", MB_CODE_2021, "<br/>",
          "<b>Population: </b>", formatC(population, format="d", big.mark=","), "<br/>",
          "<b>Accessible jobs (", cutoff_min, " min): </b>", formatC(accessibility, format="d", big.mark=",")
        ),
        group       = "MB (population > 0)"
      ) %>%
      leaflet::addPolygons(
        data        = mb_map_pop0,
        fillColor   = ~pal(accessibility),
        weight      = 0.2, color = "#666",
        opacity     = 1,   fillOpacity = 0.7,
        popup       = ~paste0(
          "<b>MB: </b>", MB_CODE_2021, "<br/>",
          "<b>Population: </b>0<br/>",
          "<b>Accessible jobs (", cutoff_min, " min): </b>", formatC(accessibility, format="d", big.mark=",")
        ),
        group       = "MB (population = 0)"
      )
    
    m <- m %>%
      leaflet::addLegend(
        pal      = pal,
        values   = c(mb_map_popgt$accessibility, mb_map_pop0$accessibility),
        opacity  = 0.7,
        title    = paste0(
          "Jobs within ", cutoff_min, " minutes",
          "<br/><span style='font-size:smaller'>(Transit, AM peak)</span>"
        ),
        position = "bottomright",
        labFormat = lab_fmt
      ) %>%
      leaflet::addLayersControl(
        overlayGroups = c("MB (population > 0)", "MB (population = 0)"),
        options = leaflet::layersControlOptions(collapsed = FALSE)
      )
    
    m  # print map
    
  } else {
    # ---------- Fallback: centroid points ----------
    mb_points       <- acc_complete
    mb_points_pop0  <- mb_points %>% dplyr::filter(population == 0)
    mb_points_popgt <- mb_points %>% dplyr::filter(population > 0)
    
    pal <- leaflet::colorBin(
      palette = "YlOrRd",
      domain  = c(mb_points_popgt$accessibility, mb_points_pop0$accessibility),
      bins    = bins,
      right   = FALSE
    )
    
    m <- leaflet::leaflet() %>%
      leaflet::addTiles() %>%
      leaflet::addCircleMarkers(
        data = mb_points_popgt,
        lng = ~lon, lat = ~lat,
        radius = 3, stroke = FALSE,
        fillOpacity = 0.6,
        fillColor = ~pal(accessibility),
        popup = ~paste0(
          "<b>MB: </b>", MB_CODE_2021, "<br/>",
          "<b>Population: </b>", formatC(population, format="d", big.mark=","), "<br/>",
          "<b>Accessible jobs (", cutoff_min, " min): </b>", formatC(accessibility, format="d", big.mark=",")
        ),
        group = "MB (population > 0)"
      ) %>%
      leaflet::addCircleMarkers(
        data = mb_points_pop0,
        lng = ~lon, lat = ~lat,
        radius = 3, stroke = FALSE,
        fillOpacity = 0.6,
        fillColor = ~pal(accessibility),
        popup = ~paste0(
          "<b>MB: </b>", MB_CODE_2021, "<br/>",
          "<b>Population: </b>0<br/>",
          "<b>Accessible jobs (", cutoff_min, " min): </b>", formatC(accessibility, format="d", big.mark=",")
        ),
        group = "MB (population = 0)"
      ) %>%
      leaflet::addLegend(
        pal      = pal,
        values   = c(mb_points_popgt$accessibility, mb_points_pop0$accessibility),
        opacity  = 0.7,
        title    = paste0(
          "Jobs within ", cutoff_min, " minutes",
          "<br/><span style='font-size:smaller'>(Transit, AM peak)</span>"
        ),
        position = "bottomright",
        labFormat = lab_fmt
      ) %>%
      leaflet::addLayersControl(
        overlayGroups = c("MB (population > 0)", "MB (population = 0)"),
        options = leaflet::layersControlOptions(collapsed = FALSE)
      )
    
    m
  }
}

# ---------------------------
# Clean up
# ---------------------------
stop_r5(r5r_core); invisible(gc())

