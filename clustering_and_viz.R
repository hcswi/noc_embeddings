library(jsonlite)
library(dplyr)
library(purrr)
library(uwot)   # For UMAP
library(dbscan) # For DBSCAN

# Load the file
file_path <- "bedrock_batch.jsonl.out"
data_list <- map(readLines(file_path), fromJSON)

library(jsonlite)
library(dplyr)

# stream_in is efficient for JSONL/NDJSON formats
df_raw <- stream_in(file("bedrock_batch.jsonl.out"))

# Flatten and select specific columns
# Note: stream_in often flattens nested names with dots (e.g., modelOutput.embedding)
df_clean <- df_raw %>%
  transmute(
    recordId = recordId,
    inputText = modelInput$inputText,
    # embedding is likely a list-column containing the 1024 numeric values
    embedding = modelOutput$embedding 
  )

df_parsed <- df_clean %>%
  mutate(
    # Extract everything before '.PDF' (the filename)
    filename = str_extract(inputText, "^[^.]+"),
    
    # Extract the digits between 'page_' and '.png'
    page_number = as.numeric(str_extract(inputText, "(?<=page_)\\d+"))
  )

colMaxs <- function(mat) apply(mat, 2, max)

doc_aggregated <- df_parsed %>%
  group_by(filename) %>%
  summarize(
    total_pages = n(),
    max_page_num = max(page_number, na.rm = TRUE),
    
    # Convert list of vectors to a matrix for calculation
    # (Assuming each 'embedding' entry is a numeric vector of length 1024)
    mean_vector = list(colMeans(do.call(rbind, embedding))),
    max_vector  = list(colMaxs(do.call(rbind, embedding)))
  ) %>%
  ungroup() 

library(uwot)

# Prepare the matrix (using mean_vector here)
mean_matrix <- do.call(rbind, doc_aggregated$mean_vector)
max_matrix <- do.call(rbind, doc_aggregated$max_vector)

# Run UMAP
umap_results_mean <- umap(mean_matrix, n_components = 2, metric = "cosine")
umap_results_max <- umap(max_matrix, n_components = 2, metric = "cosine")

# Add back to dataframe
doc_aggregated <- doc_aggregated %>%
  mutate(
    mean_umap_1 = umap_results_mean[,1],
    mean_umap_2 = umap_results_mean[,2],
    max_umap_1 = umap_results_max[,1],
    max_umap_2 = umap_results_max[,2]
  )

library(dbscan)

# Run DBSCAN
# Suggestion: Use kNNdistplot(umap_results, k = 5) first to find the best eps
db_clusters_mean <- dbscan(umap_results_mean, eps = 0.5, minPts = 3)
db_clusters_max <- dbscan(umap_results_max, eps = 0.5, minPts = 3)

doc_aggregated$cluster_mean <- as.factor(db_clusters_mean$cluster)
doc_aggregated$cluster_max <- as.factor(db_clusters_max$cluster)


library(ggplot2)
library(viridis) # For better color scales

# Ensure cluster 0 (noise) is handled visually
# In DBSCAN, cluster 0 is 'noise'. Let's label it clearly.
doc_aggregated <- doc_aggregated %>%
  mutate(cluster_label_mean = ifelse(cluster_mean == 0, "Noise", paste("Cluster", cluster_mean)),
         cluster_label_max = ifelse(cluster_max == 0, "Noise", paste("Cluster", cluster_max))) %>%
  mutate(file_url = paste0("https://pdf.hres.ca/noc_pm/", filename, ".PDF"),
         hover_text = paste0(
           "Document: ", filename, "<br>",
           "Pages: ", total_pages, "<br>",
           "Cluster ", cluster_label_mean, "<br>",
           "Click to open file"
         ))


library(pdftools)
library(dplyr)

# Function to extract text from a single file
get_pdf_text <- function(filename) {
  path <- paste0("https://pdf.hres.ca/noc_pm/", filename, ".PDF")
  # Returns a vector where each element is a page of text
  text_pages <- pdf_text(path) 
  return(paste(text_pages, collapse = "\n\n--- Page Break ---\n\n"))
}

# Apply to your existing aggregated dataframe
doc_aggregated <- doc_aggregated %>%
  mutate(full_text = sapply(filename, get_pdf_text))


ggplot(doc_aggregated, aes(x = mean_umap_1, y = mean_umap_2, color = cluster_label_mean)) +
  geom_point(aes(size = total_pages), alpha = 0.7) +
  scale_color_viridis_d(option = "plasma") +
  theme_minimal() +
  labs(
    title = "Document Semantic Landscape (UMAP + DBSCAN)",
    subtitle = "Points sized by total page count; Cluster 'Noise' represents outliers",
    x = "UMAP Dimension 1",
    y = "UMAP Dimension 2",
    color = "DBSCAN Cluster",
    size = "Pages"
  ) +
  theme(legend.position = "right")


ggplot(doc_aggregated, aes(x = max_umap_1, y = max_umap_2, color = cluster_label_max)) +
  geom_point(aes(size = total_pages), alpha = 0.7) +
  scale_color_viridis_d(option = "plasma") +
  theme_minimal() +
  labs(
    title = "Document Semantic Landscape (UMAP + DBSCAN)",
    subtitle = "Points sized by total page count; Cluster 'Noise' represents outliers",
    x = "UMAP Dimension 1",
    y = "UMAP Dimension 2",
    color = "DBSCAN Cluster",
    size = "Pages"
  ) +
  theme(legend.position = "right")

library(plotly)
library(htmlwidgets)

p <- ggplot(doc_aggregated, aes(x = mean_umap_1, y = mean_umap_2, color = cluster_label_mean, text = filename)) +
  geom_point(alpha = 0.7) +
  theme_minimal() +
  labs(title = "Interactive Document Clusters")

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

embedding_matrix <- do.call(rbind, doc_aggregated$mean_vector)

# Standardize vectors to unit length for fast cosine calculation
norm_matrix <- embedding_matrix / sqrt(rowSums(embedding_matrix^2))

# This creates a Document-by-Document similarity matrix
# Values will be between -1 and 1 (1 being identical)
sim_matrix <- norm_matrix %*% t(norm_matrix)

# Add filenames as row/column names for easy lookup
rownames(sim_matrix) <- doc_aggregated$file_url
colnames(sim_matrix) <- doc_aggregated$file_url

get_similar_docs <- function(target_filename, n_matches = 5) {
  
  if (!target_filename %in% rownames(sim_matrix)) {
    stop("Filename not found in the dataset.")
  }
  
  # Extract similarities for the specific document
  sim_scores <- sim_matrix[target_filename, ]
  
  # Sort scores in descending order and skip the first one (itself)
  similar_indices <- order(sim_scores, decreasing = TRUE)[2:(n_matches + 1)]
  
  # Return a dataframe with filenames and scores
  data.frame(
    filename = names(sim_scores)[similar_indices],
    similarity_score = as.numeric(sim_scores[similar_indices])
  )
}

library(pheatmap)

# Take a subset if the matrix is too large (e.g., first 50 docs)
pheatmap(sim_matrix[200:300, 200:300], 
         main = "Cosine Similarity Heatmap (First 50 Documents)",
         color = colorRampPalette(c("white", "blue"))(100))
