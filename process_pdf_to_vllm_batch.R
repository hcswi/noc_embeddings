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


reply_vec <- map_chr(
  qwen_resps,
  ~ if (is.null(.x$error)) get_reply(.x$result) else NA_character_
)

files <- map_chr(ok_results, "key") |> basename()
sample_nocs_qwen3_extract <- tibble(file = paste0("https://pdf.hres.ca/noc_pm/", files), resp_json = reply_vec)


result <- sample_nocs_qwen3_extract %>%
  mutate(
    parsed = map(resp_json, ~fromJSON(.x, simplifyVector = TRUE)),
    data_found = map_lgl(parsed, ~.x$data_found),
    reason = map_chr(parsed, ~ifelse(is.null(.x$reason), NA_character_, .x$reason)),
    dossier_id = map_chr(parsed, ~ifelse(is.null(.x$dossier_id), NA_character_, .x$dossier_id)),
    submission_no = map_chr(parsed, ~ifelse(is.null(.x$submission_no), NA_character_, .x$submission_no)),
    manufacturer_name = map_chr(parsed, ~{
      if(!is.null(.x$manufacturer) && is.list(.x$manufacturer)) {
        ifelse(is.null(.x$manufacturer$name), NA_character_, .x$manufacturer$name)
      } else {
        NA_character_
      }
    }),
    manufacturer_address = map_chr(parsed, ~{
      if(!is.null(.x$manufacturer) && is.list(.x$manufacturer)) {
        ifelse(is.null(.x$manufacturer$address), NA_character_, .x$manufacturer$address)
      } else {
        NA_character_
      }
    })
  ) %>%
  select(-parsed)  # Only remove the temporary parsed column

safe_extract <- function(x, ...) {
  result <- tryCatch({
    val <- x
    for (key in list(...)) {
      if (is.null(val) || !is.list(val)) return(NA_character_)
      val <- val[[key]]
    }
    if (is.null(val)) NA_character_ else as.character(val)
  }, error = function(e) NA_character_)
  return(result)
}

safe_extract_array <- function(x, ..., sep = " | ") {
  result <- tryCatch({
    val <- x
    for (key in list(...)) {
      if (is.null(val) || !is.list(val)) return(NA_character_)
      val <- val[[key]]
    }
    if (is.null(val)) {
      NA_character_
    } else if (is.vector(val) && length(val) > 1) {
      paste(val, collapse = sep)
    } else {
      as.character(val)
    }
  }, error = function(e) NA_character_)
  return(result)
}

# Parse JSON and create new columns
result <- sample_nocs_qwen3_extract %>%
  mutate(
    # Parse the JSON string
    parsed = map(resp_json, ~fromJSON(.x, simplifyVector = TRUE)),
    
    # Common field
    data_found = map_lgl(parsed, ~.x$data_found),
    
    # Fields when data_found = FALSE
    reason = map_chr(parsed, ~safe_extract(.x, "reason")),
    
    # Direct fields when data_found = TRUE (or in partial_data)
    dossier_id = map_chr(parsed, ~{
      if (!is.null(.x$dossier_id)) {
        safe_extract(.x, "dossier_id")
      } else {
        safe_extract(.x, "partial_data", "dossier_id")
      }
    }),
    
    submission_no = map_chr(parsed, ~{
      if (!is.null(.x$submission_no)) {
        safe_extract(.x, "submission_no")
      } else {
        safe_extract(.x, "partial_data", "submission_no")
      }
    }),
    
    date = map_chr(parsed, ~{
      if (!is.null(.x$date)) {
        safe_extract(.x, "date")
      } else {
        safe_extract(.x, "partial_data", "date")
      }
    }),
    
    product_name = map_chr(parsed, ~{
      if (!is.null(.x$product_name)) {
        safe_extract(.x, "product_name")
      } else {
        safe_extract(.x, "partial_data", "product_name")
      }
    }),
    
    medicinal_ingredients = map_chr(parsed, ~{
      if (!is.null(.x$medicinal_ingredients)) {
        safe_extract(.x, "medicinal_ingredients")
      } else {
        safe_extract(.x, "partial_data", "medicinal_ingredients")
      }
    }),
    
    reason_for_submission = map_chr(parsed, ~{
      if (!is.null(.x$reason_for_submission)) {
        safe_extract(.x, "reason_for_submission")
      } else {
        safe_extract(.x, "partial_data", "reason_for_submission")
      }
    }),
    
    species = map_chr(parsed, ~{
      if (!is.null(.x$species)) {
        safe_extract(.x, "species")
      } else {
        safe_extract(.x, "partial_data", "species")
      }
    }),
    
    drug_identification_number_route_form_strength = map_chr(parsed, ~{
      if (!is.null(.x$drug_identification_number_route_form_strength)) {
        safe_extract_array(.x, "drug_identification_number_route_form_strength", sep = " | ")
      } else {
        safe_extract_array(.x, "partial_data", "drug_identification_number_route_form_strength", sep = " | ")
      }
    }),
    
    
    canadian_reference_product = map_chr(parsed, ~{
      if (!is.null(.x$canadian_reference_product)) {
        safe_extract(.x, "canadian_reference_product")
      } else {
        safe_extract(.x, "partial_data", "canadian_reference_product")
      }
    }),
    
    # Manufacturer fields
    manufacturer_name = map_chr(parsed, ~{
      if (!is.null(.x$manufacturer)) {
        safe_extract(.x, "manufacturer", "name")
      } else {
        safe_extract(.x, "partial_data", "manufacturer", "name")
      }
    }),
    
    manufacturer_street = map_chr(parsed, ~{
      if (!is.null(.x$manufacturer)) {
        safe_extract(.x, "manufacturer", "street")
      } else {
        safe_extract(.x, "partial_data", "manufacturer", "street")
      }
    }),
    
    manufacturer_city = map_chr(parsed, ~{
      if (!is.null(.x$manufacturer)) {
        safe_extract(.x, "manufacturer", "city")
      } else {
        safe_extract(.x, "partial_data", "manufacturer", "city")
      }
    }),
    
    manufacturer_province = map_chr(parsed, ~{
      if (!is.null(.x$manufacturer)) {
        safe_extract(.x, "manufacturer", "province")
      } else {
        safe_extract(.x, "partial_data", "manufacturer", "province")
      }
    }),
    
    manufacturer_country = map_chr(parsed, ~{
      if (!is.null(.x$manufacturer)) {
        safe_extract(.x, "manufacturer", "country")
      } else {
        safe_extract(.x, "partial_data", "manufacturer", "country")
      }
    }),
    
    manufacturer_postal_code = map_chr(parsed, ~{
      if (!is.null(.x$manufacturer)) {
        safe_extract(.x, "manufacturer", "postal_code")
      } else {
        safe_extract(.x, "partial_data", "manufacturer", "postal_code")
      }
    }),
    
    # Authorizing official fields
    authorizing_official_name = map_chr(parsed, ~{
      if (!is.null(.x$authorizing_official)) {
        safe_extract(.x, "authorizing_official", "name")
      } else {
        safe_extract(.x, "partial_data", "authorizing_official", "name")
      }
    }),
    
    authorizing_official_title = map_chr(parsed, ~{
      if (!is.null(.x$authorizing_official)) {
        safe_extract(.x, "authorizing_official", "title")
      } else {
        safe_extract(.x, "partial_data", "authorizing_official", "title")
      }
    }),
    
    authorizing_official_division = map_chr(parsed, ~{
      if (!is.null(.x$authorizing_official)) {
        safe_extract(.x, "authorizing_official", "division")
      } else {
        safe_extract(.x, "partial_data", "authorizing_official", "division")
      }
    }),
    
    authorizing_official_directorate = map_chr(parsed, ~{
      if (!is.null(.x$authorizing_official)) {
        safe_extract(.x, "authorizing_official", "directorate")
      } else {
        safe_extract(.x, "partial_data", "authorizing_official", "directorate")
      }
    })
  ) %>%
  # Remove the temporary parsed column and optionally the original JSON
  select(-parsed)

write_csv(result, file = "sample_nocs_qwen3_extract.csv")
