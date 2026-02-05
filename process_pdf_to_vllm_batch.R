# Prep PDFs for Qwen3 VL 8B Instruct

library(paws)
library(pdftools)
library(base64enc)
library(jsonlite)
library(png)
library(dplyr)
library(purrr)
library(tidyllm)
library(readr)

# --- Configuration ---
s3_bucket      <- "swipilot-onyxia"
s3_prefix      <- "sample_nocs/"      # Include the trailing slash
output_file    <- "bedrock_claude_batch_large.jsonl"
max_tokens   <- 500
sys_prompt <- read_file("noc_system_prompt.txt")
prompt <- "Extract the notice of compliance details from the attached PDF in valid JSON format."
qwen_api_endpoint <- "http://vllm-router-service.vllm.svc.cluster.local:80"
Sys.setenv(OPENAI_API_KEY = "YOUR-OPENAI-API-KEY")
Sys.setenv(OPENAI_BASE_URL = "http://vllm-router-service.vllm.svc.cluster.local:80/v1")

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

pdftools::pdf_convert(
  pdf = tmp_pdf, 
  format = "png", 
  dpi = dpi_setting, 
  filenames = file.path(tmp_dir, "page.png"),
  verbose = FALSE
)

test_extract <- llm_message(prompt, .system_prompt = sys_prompt, .imagefile = "0000021651.png")

test_extract <- pdf_page_batch("0000021651.PDF", prompt, sys_prompt, c(1,1))

qwen_resp <- test_extract[[1]] |> openai_chat(.model = "Qwen/Qwen3-VL-8B-Instruct",
      .api_url = qwen_api_endpoint,
      .tools = list(),
      .tool_choice = "none")
  
get_reply(qwen_resp)

library(purrr)
library(progressr)

handlers(global = TRUE)
handlers("rstudio")

qwen_resps <- with_progress({
  p <- progressor(along = messages_list)
  
  map(messages_list, \(msg) {
    p()
    safely(\(x) x |>
             openai_chat(
               .model = "Qwen/Qwen3-VL-8B-Instruct",
               .api_url = qwen_api_endpoint,
               .tools = list(),
               .tool_choice = "none"
             )
    )(msg)
  })
})

# qwen_resps[[i]]$result or qwen_resps[[i]]$error

process_one_pdf <- function(key) {
  # ---- download from S3 ----
  tmp_file <- tempfile(fileext = ".pdf")
  s3$download_file(
    Key = key,
    Bucket = s3_bucket,
    Filename = tmp_file
  )
  
  # ---- extract page 1 with pdf_page_batch ----
  # pdf_page_batch returns a tibble of (page_number, content, prompt, etc.)
  batch <- pdf_page_batch(
    .pdf = tmp_file,
    .page_range = c(1,1),                   # <-- only first page
    .general_prompt = prompt,
    .system_prompt = sys_prompt
  )
  return(batch[[1]])
}

messages_list <- map(pdf_keys[100:110], process_one_pdf)



library(purrr)

process_one_pdf <- function(key) {
  tmp_file <- tempfile(fileext = ".pdf")
  
  # Ensure temp file is cleaned up even if something errors
  on.exit({
    if (file.exists(tmp_file)) unlink(tmp_file)
  }, add = TRUE)
  
  # ---- try once: download + extract ----
  res <- tryCatch({
    # download from S3
    s3$download_file(
      Key      = key,
      Bucket   = s3_bucket,
      Filename = tmp_file
    )
    
    # extract page 1 with pdf_page_batch
    batch <- pdf_page_batch(
      .pdf            = tmp_file,
      .page_range     = c(1, 1),   # only first page
      .general_prompt = prompt,
      .system_prompt  = sys_prompt
    )
    
    # IMPORTANT: keep your original behavior
    # you returned batch[[1]] (first row/element)
    list(
      ok       = TRUE,
      key      = key,
      messages = batch[[1]],
      error    = NULL
    )
  }, error = function(e) {
    list(
      ok       = FALSE,
      key      = key,
      messages = NULL,
      error    = conditionMessage(e)
    )
  })
  
  res
}

results <- map(pdf_keys, process_one_pdf)

# Convenience views:
ok_results    <- keep(results, ~ .x$ok)
bad_results   <- keep(results, ~ !.x$ok)
messages_list <- map(ok_results, "messages")   # only successful message payloads
errors_list   <- map(bad_results, ~ list(key = .x$key, error = .x$error))


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

s3$put_object(Bucket = s3_bucket, 
              Key = paste0("bedrock_claude_batch_500/", basename("bedrock_claude_batch_large.jsonl")),
              Body = "bedrock_claude_batch_large.jsonl")