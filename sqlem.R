chunkData <- function(file, blocksize = 500) {
  numBlocks = ceiling(dim(file)[1] / blocksize)
  split(file, factor(sort(rank(row.names(
    file
  )) %% numBlocks)))
}

queryBuilder <- function(chunk, name) {
  chunk[] <- sapply(chunk, function(x) {
    paste0("'", trimws(x), "'")
  })
  chunk[] <- Map(paste, chunk, names(chunk), sep = ' as ')
  sql_hdr <- paste0(name, " as ( ")
  chunk <- apply(chunk, 1, function(x)
    paste(x, collapse = ", "))
  chunk <- paste0("(select ", chunk, " from dual)")
  paste(sql_hdr, paste(chunk, collapse = "  UNION ALL \n"), "),")
}

sqlMaker <- function(file) {
  chunkfile <- chunkData(file)
  filename <- deparse(substitute(file))
  names(chunkfile) <- paste0(filename, 1:length(chunkfile))
  paste(lapply(1:length(chunkfile), function(i)
    queryBuilder(chunk = chunkfile[[i]], name = names(chunkfile[i]))), collapse = " ")
}

makeSQL <- function(data) {
  name = paste0(deparse(substitute(data)), ".sql")
  query <- sqlMaker(data)
  fileConn <-
    file(name)
  writeLines(query, fileConn)
  close(fileConn)
}
