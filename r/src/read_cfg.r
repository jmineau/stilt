#' read_cfg parses a CFG file into a named list.
#' @author Ben Fasoli
#'
#' Reads CFG hysplit output file into named list
#'
#' @param file location of CFG file
#' @return A named list containing the parameters from the CFG file.
#'
#' @export

read_cfg <- function(file) {

  n_lines <- count_lines(file)

  if (n_lines < 2) {
    warning(paste('read_cfg(): only 1 line found in', file))
    return(NULL)
  }

  lines_with_equals <- grep('=', readLines(file), fixed = T, value = T)
  key_value_pairs <- strsplit(gsub('\\s+|\'|,', '', lines_with_equals), '=')

  namelist <- list()
  for (i in 1:length(key_value_pairs)) {
    key <- key_value_pairs[[i]][1]
    val <- key_value_pairs[[i]][2]
    namelist[[key]] <- type.convert(val, as.is = T)
  }

  namelist
}
