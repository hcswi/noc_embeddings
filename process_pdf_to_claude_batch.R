# Prep PDFs for Claude Sonnet batch inference
# https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-anthropic-claude-messages.html

library(paws)
library(pdftools)
library(base64enc)
library(jsonlite)
library(png)
library(dplyr)
library(purrr)

# --- Configuration ---
s3_bucket      <- "swipilot-onyxia"
s3_prefix      <- "sample_nocs/"      # Include the trailing slash
output_file    <- "bedrock_claude_batch_large.jsonl"
max_tokens   <- 500
sys_prompt <- read_file("noc_system_prompt.txt")
prompt <- "Extract the notice of compliance details from the attached PDF in valid JSON format."

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

# finished_pdfs <- stream_in(file("bedrock_batch_large.jsonl")) %>% 
#   `[`("recordId") %>%
#   mutate(pdf_keys = paste0("noc_dump_20260115/", str_replace(recordId, regex("PDFP\\d+"), "\\.PDF"))) %>%
#   distinct(pdf_keys)
# pdf_keys <- tibble(pdf_keys = pdf_keys) %>%
#   anti_join(finished_pdfs) %>%
#   pull(pdf_keys)

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
    
    file_name_only <- basename(key)
    
    b64_string <- base64enc::base64encode(tmp_pdf)
      
    # Prepare Clean recordId (Alphanumeric only)
    record_id <- gsub("[^[:alnum:]]", "", file_name_only)
    
      
    # Construct Bedrock JSON
    record <- list(
      recordId = record_id,
      modelInput = list(
        anthropic_version = "bedrock-2023-05-31",
        max_tokens = max_tokens,
        system = sys_prompt,
        messages = list(
          role = "user",
          content = list(
            list(
              type = "image",
              media_type = "application/pdf",
              data = b64_string
            ),
            list(
              type = "text",
              text = prompt
            )
          )
        )
        )
      )
      
      writeLines(jsonlite::toJSON(record, auto_unbox = TRUE), con)

  }, error = function(e) {
    message("Failed to process ", key, ": ", e$message)
  }, finally = {
    # 5. Recursively remove the temp directory and its images
    unlink(tmp_dir, recursive = TRUE)
  })
}

close(con)
message("Batch file '", output_file, "' is ready for upload.")

