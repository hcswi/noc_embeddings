library(paws)
library(base64enc)
library(jsonlite)
library(aws.signature)
library(dplyr)
library(purrr)
library(stringr)

# need to run apt-get install libpoppler-cpp-dev


# --- Configuration ---
s3_bucket      <- "hcpath-onyxia"
s3_prefix      <- "project-swipilot/cbsa_model2/image_embeddings/"      # Include the trailing slash

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
    region = Sys.getenv("AWS_DEFAULT_REGION"),
    signature_version = "v4")
)

# 1. List all PDF objects in the bucket
response <- paginate(s3$list_objects_v2(Bucket = s3_bucket, Prefix = s3_prefix))
json_keys <- map(response, \(page) map_chr(page$Contents, "Key")) %>% flatten_chr()
json_keys <- json_keys[grepl("\\.json$", json_keys, ignore.case = TRUE)]

load_s3_json <- function(key, bucket) {
  resp <- s3$get_object(Bucket = s3_bucket, Key = key)
  
  # Parse everything
  raw_data <- resp$Body %>% 
    rawToChar() %>% 
    jsonlite::fromJSON()
  
  # Identify your vector field (change "embedding" to your actual key name)
  # Wrapping it in list() prevents tibble from expanding it to 1024 rows
  raw_data$page_1_titan_embedding <- list(as.numeric(raw_data$page_1_titan_embedding))
  
  return(as_tibble(raw_data))
}

# Run the batch load
final_df <- json_keys %>%
  purrr::map(\(k) load_s3_json(k, s3_bucket), .progress = "Downloading") %>%
  purrr::list_rbind()

final_df_filter <- filter(final_df, !is.na(image_reference_number)) %>% 
  select(page_1_titan_embedding, starts_with("image_"), pdf_total_pages, s3_url, date_indexed)

embedding_matrix <- do.call(rbind, final_df_filter$page_1_titan_embedding)
rownames(embedding_matrix) <- final_df_filter$s3_url

library(uwot)

# Note: set.seed for reproducibility
set.seed(42)
umap_results <- uwot::umap(
  embedding_matrix,
  nn_method = "hnsw",
  n_neighbors = 15,
  n_components = 2,
  min_dist = 0.1, 
  metric = "cosine", # Use cosine for embeddings (standard for Titan)
  n_threads = parallel::detectCores() - 1, # Use multiple CPU cores
  verbose = TRUE
)

# 5. Combine results back for plotting
umap_df <- as.data.frame(umap_results)
colnames(umap_df) <- c("UMAP1", "UMAP2")
umap_df$s3_url <- rownames(embedding_matrix) 
umap_df <- umap_df %>% left_join(final_df_filter)

eccrd_50_image_type_codes <- select(eccrd_50_image_type_codes, image_type_code = Image_Type_Code, document_type_description = Description, program = Program) 

umap_df <- umap_df %>% left_join(eccrd_50_image_type_codes %>% mutate_all(as.character))
library(readr)
write_csv(umap_df, file = "umap_df_20260511.csv")

s3$put_object(Bucket = s3_bucket,
              Key = paste0(s3_prefix, "umap_df_20260511.csv"),
              Body = readBin("umap_df_20260511.csv", "raw", n = file.info("umap_df_20260511.csv")$size),
              ContentType = "text/csv")

library(plotly)
library(htmlwidgets)

p <- ggplot(umap_df, aes(x = UMAP1, y = UMAP2, color = image_type_code)) +
  geom_point(alpha = 0.7) +
  theme_minimal() +
  labs(title = "Interactive Document Clusters")

p <- ggplot(umap_df, aes(x = UMAP1, y = UMAP2, color = document_type_description)) +
  # Use a smaller stroke and size for 100k points
  geom_point(alpha = 0.7, size = 0.5) + 
  theme_minimal() +
  labs(title = "Document Clusters by Program",
       x = "UMAP Dimension 1",
       y = "UMAP Dimension 2",
       color = "Doc Type") +
  # Add the faceting here
  # ncol = 4 ensures it doesn't get too wide; scales = "fixed" keeps clusters comparable
  facet_wrap(~program, ncol = 4) +
  # Improve legend and label legibility
  theme(legend.position = "bottom",
        strip.text = element_text(face = "bold"))

md_plot <- filter(umap_df, program == "HC Medical Device") %>% 
  ggplot(aes(x = UMAP1, y = UMAP2, color = document_type_description)) +
  # Use a smaller stroke and size for 100k points
  geom_point(alpha = 0.7, size = 0.7) + 
  theme_minimal() +
  labs(title = "Medical Device LPCO",
       x = "UMAP Dimension 1",
       y = "UMAP Dimension 2",
       color = "Doc Type")

ocs_plot <- filter(umap_df, program == "HC Office of Controlled Substances") %>% 
  ggplot(aes(x = UMAP1, y = UMAP2, color = document_type_description)) +
  # Use a smaller stroke and size for 100k points
  geom_point(alpha = 0.7, size = 0.7) + 
  theme_minimal() +
  labs(title = "Office of Controlled Substances LPCO",
       x = "UMAP Dimension 1",
       y = "UMAP Dimension 2",
       color = "Doc Type")



human_drugs_plot <- filter(umap_df, program == "HC Human Drugs") %>% 
  ggplot(aes(x = UMAP1, y = UMAP2, color = document_type_description)) +
  # Use a smaller stroke and size for 100k points
  geom_point(alpha = 0.7, size = 0.7) + 
  theme_minimal() +
  labs(title = "HC Human Drugs LPCO",
       x = "UMAP Dimension 1",
       y = "UMAP Dimension 2",
       color = "Doc Type")

umap_df_thumbs <- umap_df %>%
  mutate(presigned_url = map_chr(s3_url, function(url_string) {
    # Extract the key from the full S3 URL if you don't have a 'key' column
    # This regex assumes your key is everything after the bucket name
    key <- gsub("https://.*\\.s3\\..*\\.amazonaws\\.com/", "", url_string)
    
    s3$generate_presigned_url(
      client_method = "get_object",
      params = list(Bucket = s3_bucket, Key = key),
      expires_in = 3600
    )
  }, .progress = "Signing URLs"))

# This opens an interactive window in your RStudio viewer
ggplotly(p, tooltip = "text")


p <- plot_ly(
  data = doc_aggregated,
  x = ~mean_umap_1,
  y = ~mean_umap_2,
  color = ~cluster_label_mean,
  type = 'scatter',
  mode = 'markers',
  marker = list(size = 12, opacity = 0.8),
  text = ~hover_text,
  customdata = ~file_url, # Pass the URL to the JS layer
  hoverinfo = 'text'
) %>%
  layout(
    title = "Semantic Document Map (Click a point to open PDF)",
    hovermode = "closest"
  ) %>%
  # Add JavaScript to handle the click
  onRender("
    function(el) {
      el.on('plotly_click', function(data) {
        var url = data.points[0].customdata;
        window.open(url, '_blank');
      });
    }
  ")

p
saveWidget(p, "noc_semantic_map.html", selfcontained = TRUE)

