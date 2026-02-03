library(paws)
library(pdftools)
library(base64enc)
library(jsonlite)
library(png)

# --- Configuration ---
s3_bucket      <- "swipilot-onyxia"
s3_prefix      <- "sample_nocs/"      # Include the trailing slash

# Initialize S3 client
svc <- paws::textract(config = list(
  credentials = list(
    creds = list(
      access_key_id = Sys.getenv("AWS_ACCESS_KEY_ID"),
      secret_access_key = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
      session_token = Sys.getenv("AWS_SESSION_TOKEN")
    )),
  region = Sys.getenv("AWS_DEFAULT_REGION"))
)


result <- tryCatch({
svc$analyze_document(
  Document = list(S3Object = list(Bucket = s3_bucket,
                                  Name = "sample_nocs/0000001694.PDF")),
  FeatureTypes = list("FORMS", "QUERIES", "LAYOUT"),
  QueriesConfig = list(
    Queries = list(
      list(Text = "What is the Dossier Id?",
           Alias = "dossier_id"
    ),
      list(Text = "What is the Submission Number?",
           Alias = "submission_no"),
      list(Text = "Who is the Sponsor/Manufacturer?",
           Alias = "manufacturer_name"),
      list(Text = "What is the Product name?",
           Alias = "product_name"),
      list(Text = "What are the medicinal ingredients?",
           Alias = "medicinal_ingredients"),
      list(Text = "What is the Drug Identification Number, Route, Form and Strength?",
           Alias = "din_route_form_strength"),
      list(Text = "What is the Canadian Reference Product?",
           Alias = "canadian_reference_product"),
      list(Text = "What is the date of the document?",
           Alias = "noc_date"),
      list(Text = "What is the species?",
           Alias = "species"),
      list(Text = "What is the directorate that approved the submission?",
           Alias = "directorate"),
      list(Text = "What was the reason for the submission?",
           Alias = "noc_reason"),
      list(Text = "What is the name of the Director General that signed this NOC?",
           Alias = "approving_dg")
      )
    )
)
}, error = function(e) {
  # This prints the actual error message hidden in the XML/Response
  message("AWS Error Caught:")
  print(e$message) 
  return(NULL)
})