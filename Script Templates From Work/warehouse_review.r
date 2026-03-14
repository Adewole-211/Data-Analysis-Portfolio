## LOAD LIBRARIES
####################################################################################################
install.packages("tidyverse")
install.packages("ggplot2")
install.packages("skimr")
install.packages("dplyr")
install.packages("lubridate")
install.packages("openxlsx")
install.packages("gghighlight")
install.packages("scales")

library("tidyr")
library("ggplot2")
library("skimr")
library("dplyr")
library("lubridate")
library("openxlsx")
library("scales")
library("hrbrthemes")
library("patchwork")

## DATA INGESTION
####################################################################################################
metric_a_data <- read.csv("path/to/metric_a.csv")
metric_b_data <- read.csv("path/to/metric_b.csv")
metric_c_data <- read.csv("path/to/metric_c.csv")
metric_d_data <- read.csv("path/to/metric_d.csv")
metric_a_root_cause <- read.csv("path/to/metric_a_root_cause.csv")

colnames(metric_a_data)
colnames(metric_b_data)
colnames(metric_c_data)
colnames(metric_d_data)
colnames(metric_a_root_cause)

colnames(metric_a_data) <- c("site", "region", "week", "metric_a_rate")
colnames(metric_b_data) <- c(
  "site", "group_1", "region", "target",
  "week_number", "week", "date",
  "metric_b_value", "metric_b_last_year", "yoy_change"
)
colnames(metric_c_data) <- c(
  "site", "group_2", "region", "target_rate",
  "week_number", "week", "date",
  "metric_c_count", "metric_c_rate", "vs_target", "wow", "yoy"
)
colnames(metric_d_data) <- c(
  "site", "group_1", "region", "target_rate",
  "week_number", "week", "full_date",
  "units", "metric_d_rate", "vs_target", "yoy"
)
colnames(metric_a_root_cause) <- c(
  "site", "start", "end", "process_path",
  "category", "subcategory", "root_cause",
  "commentary", "metric_a_bps"
)

View(metric_a_data)
View(metric_b_data)
View(metric_c_data)
View(metric_d_data)
View(metric_a_root_cause)

####################################################################################################
## CREATE TABLES
####################################################################################################

## FOR METRIC A
metric_a_wide <- metric_a_data %>%
  pivot_wider(
    names_from = week,
    values_from = metric_a_rate
  )

metric_a_wide <- metric_a_wide %>%
  select(site, all_of(as.character(sort(as.integer(setdiff(names(.), "site"))))))

metric_a_total <- metric_a_data %>%
  group_by(site) %>%
  summarise(avg_metric_a = mean(metric_a_rate, na.rm = TRUE))

metric_a_data$metric_a_rate <- percent(metric_a_data$metric_a_rate, accuracy = 1)
metric_a_wide[2:5] <- sapply(metric_a_wide[2:5], function(x) percent(x, accuracy = 1))
metric_a_total$avg_metric_a <- percent(metric_a_total$avg_metric_a, accuracy = 1)

write.xlsx(
  list(
    dashboard = metric_a_data,
    wide = metric_a_wide,
    total = metric_a_total
  ),
  file = "metric_a_summary.xlsx"
)

####################################################################################################

## FOR METRIC B
metric_b_data <- separate(metric_b_data, date, c("date", "time"), sep = " ")
metric_b_data$date <- ymd(metric_b_data$date)
metric_b_data$weekday <- weekdays(metric_b_data$date)

colnames(metric_b_data)[is.na(colnames(metric_b_data)) | colnames(metric_b_data) == ""] <-
  paste0("temp_col_", seq_along(which(is.na(colnames(metric_b_data)) | colnames(metric_b_data) == "")))

metric_b_wide <- metric_b_data %>%
  mutate(metric_b_value = as.numeric(metric_b_value)) %>%
  filter(!is.na(metric_b_value), !is.na(week_number)) %>%
  select(site, week_number, metric_b_value) %>%
  pivot_wider(
    names_from = week_number,
    values_from = metric_b_value,
    values_fill = 0,
    values_fn = sum
  )

metric_b_wide <- metric_b_wide %>%
  select(site, all_of(as.character(sort(as.integer(setdiff(names(.), "site"))))))

metric_b_total <- metric_b_data %>%
  group_by(site) %>%
  summarise(total_metric_b = sum(metric_b_value, na.rm = TRUE))

write.xlsx(
  list(
    dashboard = metric_b_data,
    wide = metric_b_wide,
    total = metric_b_total
  ),
  file = "metric_b_summary.xlsx"
)

####################################################################################################

## FOR METRIC C
metric_c_data <- separate(metric_c_data, date, c("date", "time"), sep = "T")
metric_c_data$date <- ymd(metric_c_data$date)
metric_c_data$weekday <- weekdays(metric_c_data$date)

colnames(metric_c_data)[is.na(colnames(metric_c_data)) | colnames(metric_c_data) == ""] <-
  paste0("temp_col_", seq_along(which(is.na(colnames(metric_c_data)) | colnames(metric_c_data) == "")))

metric_c_wide <- metric_c_data %>%
  mutate(metric_c_count = as.numeric(metric_c_count)) %>%
  filter(!is.na(metric_c_count), !is.na(week_number)) %>%
  select(site, week_number, metric_c_count) %>%
  pivot_wider(
    names_from = week_number,
    values_from = metric_c_count,
    values_fill = 0,
    values_fn = sum
  )

metric_c_wide <- metric_c_wide %>%
  select(site, all_of(as.character(sort(as.integer(setdiff(names(.), "site"))))))

metric_c_total <- metric_c_data %>%
  group_by(site) %>%
  summarise(total_metric_c = sum(metric_c_count, na.rm = TRUE))

write.xlsx(
  list(
    dashboard = metric_c_data,
    wide = metric_c_wide,
    total = metric_c_total
  ),
  file = "metric_c_summary.xlsx"
)

####################################################################################################

## FOR METRIC D
metric_d_data <- separate(metric_d_data, full_date, c("date", "time"), sep = " ")
metric_d_data$date <- ymd(metric_d_data$date)
metric_d_data$weekday <- weekdays(metric_d_data$date)

colnames(metric_d_data)[is.na(colnames(metric_d_data)) | colnames(metric_d_data) == ""] <-
  paste0("temp_col_", seq_along(which(is.na(colnames(metric_d_data)) | colnames(metric_d_data) == "")))

metric_d_wide <- metric_d_data %>%
  mutate(units = as.numeric(units)) %>%
  filter(!is.na(units), !is.na(week_number)) %>%
  select(site, week_number, units) %>%
  pivot_wider(
    names_from = week_number,
    values_from = units,
    values_fill = 0,
    values_fn = sum
  )

metric_d_wide <- metric_d_wide %>%
  select(site, all_of(as.character(sort(as.integer(setdiff(names(.), "site"))))))

metric_d_total <- metric_d_data %>%
  group_by(site) %>%
  summarise(total_metric_d = sum(units, na.rm = TRUE))

write.xlsx(
  list(
    dashboard = metric_d_data,
    wide = metric_d_wide,
    total = metric_d_total
  ),
  file = "metric_d_summary.xlsx"
)

####################################################################################################
## ROOT CAUSE ANALYSIS
metric_a_root_cause <- separate(metric_a_root_cause, start, c("start_date", "start_time"), sep = " ")
metric_a_root_cause <- separate(metric_a_root_cause, end, c("end_date", "end_time"), sep = " ")

metric_a_root_cause$start_date <- ymd(metric_a_root_cause$start_date)
metric_a_root_cause$end_date <- ymd(metric_a_root_cause$end_date)

metric_a_root_cause$start_weekday <- weekdays(metric_a_root_cause$start_date)
metric_a_root_cause$end_weekday <- weekdays(metric_a_root_cause$end_date)

metric_a_rc_by_process <- metric_a_root_cause %>%
  group_by(process_path, root_cause) %>%
  summarise(bps = sum(metric_a_bps, na.rm = TRUE), .groups = "drop")

metric_a_rc_total <- metric_a_root_cause %>%
  group_by(root_cause) %>%
  summarise(bps = sum(metric_a_bps, na.rm = TRUE))

write.xlsx(
  list(
    dashboard = metric_a_root_cause,
    by_process = metric_a_rc_by_process,
    total_root_cause = metric_a_rc_total
  ),
  file = "metric_a_root_cause_summary.xlsx"
)

####################################################################################################
## VISUALISATION

View(metric_a_rc_total)

ggplot(data = metric_a_rc_total) +
  geom_col(mapping = aes(x = root_cause, y = bps), stat = "identity")

root_cause_bps_plot <- ggplot(data = metric_a_rc_total) +
  geom_col(
    mapping = aes(x = root_cause, y = bps),
    stat = "identity",
    fill = "#86b7f0",
    colour = "black",
    width = 0.45
  ) +
  geom_text(aes(x = root_cause, y = bps, label = bps), vjust = -0.5, size = 8) +
  theme_dark() +
  theme(
    plot.background = element_rect(fill = "grey", colour = "black"),
    plot.title = element_text(size = 22),
    axis.text.x = element_text(angle = 45, hjust = 1, size = 15),
    axis.text.y = element_text(size = 15),
    axis.title = element_text(size = 18)
  ) +
  labs(
    title = "Root Cause Contribution",
    x = "Root Cause",
    y = "BPS"
  )

root_cause_bps_plot

####################################################################################################
## WEEK-OVER-WEEK VISUALS

metric_c_graph <- metric_c_data %>%
  filter(site == "SITE_A") %>%
  group_by(week) %>%
  summarise(metric_c_count = sum(metric_c_count, na.rm = TRUE))

ggplot(data = metric_c_graph, aes(x = week, y = metric_c_count, fill = week)) +
  geom_col(alpha = 0.7) +
  theme_dark() +
  theme(
    panel.background = element_rect(colour = "black"),
    plot.title = element_text(size = 22, hjust = 0.5),
    axis.text.x = element_text(angle = 45, hjust = 1, size = 15),
    axis.text.y = element_text(size = 15),
    axis.title = element_text(size = 18)
  ) +
  geom_text(aes(label = metric_c_count), vjust = -1.5, size = 5) +
  labs(
    x = "Week Number",
    y = "Metric C Count",
    title = "Site A Metric C Week-over-Week"
  )

metric_a_graph <- metric_a_data %>%
  filter(site == "SITE_A") %>%
  group_by(week) %>%
  summarise(metric_a_rate = median(metric_a_rate, na.rm = TRUE))

ggplot(data = metric_a_graph, aes(x = week, y = metric_a_rate, fill = week)) +
  geom_col(alpha = 0.7) +
  theme_dark() +
  theme(
    panel.background = element_rect(colour = "black"),
    plot.title = element_text(size = 22, hjust = 0.5),
    axis.text.x = element_text(angle = 45, hjust = 1, size = 15),
    axis.text.y = element_text(size = 15),
    axis.title = element_text(size = 18)
  ) +
  geom_text(aes(label = metric_a_rate), vjust = -1.5, size = 5) +
  labs(
    x = "Week Number",
    y = "Average Metric A",
    title = "Site A Metric A Week-over-Week"
  )

metric_b_graph <- metric_b_data %>%
  filter(site == "SITE_A") %>%
  group_by(week) %>%
  summarise(metric_b_value = round(sum(metric_b_value, na.rm = TRUE), 3))

ggplot(data = metric_b_graph, aes(x = week, y = metric_b_value, fill = week)) +
  geom_col(alpha = 0.7) +
  theme_dark() +
  theme(
    panel.background = element_rect(colour = "black"),
    plot.title = element_text(size = 22, hjust = 0.5),
    axis.text.x = element_text(angle = 45, hjust = 1, size = 15),
    axis.text.y = element_text(size = 15),
    axis.title = element_text(size = 18)
  ) +
  geom_text(aes(label = metric_b_value), vjust = -1.5, size = 5) +
  labs(
    x = "Week Number",
    y = "Metric B Value",
    title = "Site A Metric B Week-over-Week"
  )

metric_d_graph <- metric_d_data %>%
  filter(site == "SITE_A") %>%
  group_by(week) %>%
  summarise(units = sum(units, na.rm = TRUE))

ggplot(data = metric_d_graph, aes(x = week, y = units, fill = week)) +
  geom_col(alpha = 0.7) +
  theme_dark() +
  theme(
    panel.background = element_rect(colour = "black"),
    plot.title = element_text(size = 22, hjust = 0.5),
    axis.text.x = element_text(angle = 45, hjust = 1, size = 15),
    axis.text.y = element_text(size = 15),
    axis.title = element_text(size = 18)
  ) +
  geom_text(aes(label = units), vjust = -1.5, size = 5) +
  labs(
    x = "Week Number",
    y = "Units",
    title = "Site A Metric D Week-over-Week"
  )
####################################################################################################
