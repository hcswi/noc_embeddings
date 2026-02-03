# Download NOCs
library(readr)
library(dplyr)
library(purrr)
library(cli)
library(aws.signature)
library(paws)

creds <- locate_credentials(region = "ca-central-1")
# Initialize S3 client
s3 <- paws::s3(
  config = list(
    credentials = list(
      creds = list(
        access_key_id = creds$key,
        secret_access_key = creds$secret,
        session_token = creds$session_token
      )),
    region = creds$region
  )
)


noc_list <- "https://pdf.hres.ca/noc_pm/noc_pm.pdf.a4e6e15484a061528e396a3e4aebd6f0.list"
s3_bucket <- "swipilot-onyxia"
s3_prefix <- "noc_dump_20260115/"

s3_list <- tibble(path = pdf_keys) %>%
          mutate(filename = basename(path))

noc_fetch <- read_csv(noc_list, col_names = "filename") %>%
             tibble() %>%
             anti_join(s3_list) %>%
             mutate(fetch_url = paste0("https://pdf.hres.ca/noc_pm/", filename)) %>%
  pwalk(function(filename, fetch_url, ...) {
    tmp <- tempfile(fileext = ".pdf")
    
    # Download file
    download.file(fetch_url, tmp, mode = "wb", quiet = TRUE)
    
    # Upload to S3
    s3$put_object(
      Bucket = s3_bucket,
      Key = paste0(s3_prefix, filename),
      Body = tmp
    )
    
    # Clean up local temp file
    unlink(tmp)
  }, .progress = "Uploading to S3")

