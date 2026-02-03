library(paws)
library(pdftools)
library(base64enc)
library(jsonlite)
library(png)
library(aws.signature)
library(dplyr)
library(purrr)
library(stringr)

# need to run apt-get install libpoppler-cpp-dev


# --- Configuration ---
s3_bucket      <- "swipilot-onyxia"
s3_prefix      <- "noc_dump_20260115/"      # Include the trailing slash
output_file    <- "bedrock_batch_large.jsonl"
dpi_setting    <- 150
embed_length   <- 1024



# creds <- locate_credentials(region = "ca-central-1")
# Initialize S3 client
s3 <- paws::s3(
  config = list(
    credentials = list(
      creds = list(
        access_key_id = Sys.getenv("AWS_ACCESS_KEY_ID"),
        secret_access_key = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
        session_token = Sys.getenv("AWS_SESSION_TOKEN")
      )),
    endpoint = paste0("https://", Sys.getenv("AWS_S3_ENDPOINT")),
    region = Sys.getenv("AWS_DEFAULT_REGION"))
    )

# 1. List all PDF objects in the bucket
response <- paginate(s3$list_objects_v2(Bucket = s3_bucket, Prefix = s3_prefix))
pdf_keys <- map(response, \(page) map_chr(page$Contents, "Key")) %>% flatten_chr()
pdf_keys <- pdf_keys[grepl("\\.pdf$", pdf_keys, ignore.case = TRUE)]

finished_pdfs <- stream_in(file("bedrock_batch_large.jsonl")) %>% 
                  `[`("recordId") %>%
                  mutate(pdf_keys = paste0("noc_dump_20260115/", str_replace(recordId, regex("PDFP\\d+"), "\\.PDF"))) %>%
                  distinct(pdf_keys)
pdf_keys <- tibble(pdf_keys = pdf_keys) %>%
            anti_join(finished_pdfs) %>%
            pull(pdf_keys)

# Open connection to the JSONL output file
con <- file(output_file, open = "a")

message(sprintf("Found %d PDFs. Starting processing...", length(pdf_keys)))

# --- Processing Loop ---
for (key in pdf_keys) {
  message("Processing: ", key)
  
  # 1. Create a workspace for this specific PDF
  tmp_dir <- tempfile(pattern = "pdf_work_")
  dir.create(tmp_dir)
  tmp_pdf <- file.path(tmp_dir, "input.pdf")
  
  tryCatch({
    # 2. Download from S3
    s3$download_file(Bucket = s3_bucket, Key = key, Filename = tmp_pdf)
    
    # 3. Convert ALL pages to PNG files in the temp directory
    # This returns a character vector of the paths to the generated images
    image_paths <- pdftools::pdf_convert(
      pdf = tmp_pdf, 
      format = "png", 
      dpi = dpi_setting, 
      filenames = file.path(tmp_dir, "page.png"),
      verbose = FALSE
    )
    
    file_name_only <- basename(key)
    
    # 4. Loop through the generated PNG files
    for (i in seq_along(image_paths)) {
      img_path <- image_paths[i]
      
      # Encode image to Base64
      b64_string <- base64enc::base64encode(img_path)
      
      # Prepare Clean recordId (Alphanumeric only)
      clean_name <- gsub("[^[:alnum:]]", "", file_name_only)
      record_id  <- paste0(clean_name, "P", i)
      
      # Construct Bedrock JSON
      record <- list(
        recordId = record_id,
        modelInput = list(
          inputText = paste0(file_name_only, "_page_", i, ".png"),
          inputImage = b64_string,
          embeddingConfig = list(outputEmbeddingLength = embed_length)
        )
      )
      
      writeLines(jsonlite::toJSON(record, auto_unbox = TRUE), con)
    }
    
  }, error = function(e) {
    message("Failed to process ", key, ": ", e$message)
  }, finally = {
    # 5. Recursively remove the temp directory and its images
    unlink(tmp_dir, recursive = TRUE)
  })
}

close(con)
message("Batch file '", output_file, "' is ready for upload.")

