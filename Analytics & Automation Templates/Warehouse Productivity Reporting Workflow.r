## LOAD LIBRARIES ##
############################################################################################################################################
install.packages("tidyverse")
install.packages("ggplot2")
install.packages("skimr")
install.packages("dplyr")
install.packages("lubridate")
install.packages("openxlsx")
install.packages("gghighlight")
install.packages("scales")
install.packages("httpgd")
install.packages("languageserver")

install.packages("IRkernel")
IRkernel::installspec(user = FALSE)

############################################################################################################################################

library("tidyr")
library("ggplot2")
library("skimr")
library("dplyr")
library("lubridate")
library("openxlsx")
library("scales")
library("hrbrthemes")
library("patchwork")
library(httpgd)

hgd()

## Formatting and Standardising ##
############################################################################################################################################

site1_metric_a <- read.csv("metric_a.csv")
site1_metric_b <- read.csv("metric_b.csv")
site1_metric_c <- read.csv("metric_c.csv")
site1_metric_d <- read.csv("metric_d.csv")
site1_metric_e <- read.csv("metric_e.csv")

############################################################################################################################################

colnames(site1_metric_a)
colnames(site1_metric_b)
colnames(site1_metric_c)
colnames(site1_metric_d)
colnames(site1_metric_e)

colnames(site1_metric_a) <- c("fc", "week", "variance_pct")
colnames(site1_metric_b) <- c("warehouse_id", "period", "region", "year_number", "issue_category", "impact_level", "performance_score", "total_value")
colnames(site1_metric_c) <- c("record_date", "fc", "missed_count", "unit_count", "deviation_score")
colnames(site1_metric_d) <- c("warehouse_id", "ship_date", "units")
colnames(site1_metric_e) <- c("dow", "hour", "total_score")

View(site1_metric_a)
View(site1_metric_b)
View(site1_metric_c)
View(site1_metric_d)
View(site1_metric_e)

################################################################################################################################################################
## CREATE TABLES ##
################################################################################################################################################################
## METRIC A ##

site1_metric_a$variance_pct <- as.numeric(site1_metric_a$variance_pct)

site1_metric_a_wide <- site1_metric_a %>%
  pivot_wider(
    names_from  = week,
    values_from = variance_pct,
    values_fn   = mean
  )

site1_metric_a_total <- site1_metric_a %>%
  group_by(fc) %>%
  summarise(avg_variance = mean(variance_pct, na.rm = TRUE))

View(site1_metric_a_wide)
View(site1_metric_a_total)

write.xlsx(
  list(
    dashboard = site1_metric_a,
    wide      = site1_metric_a_wide,
    total     = site1_metric_a_total
  ),
  file = "site1_metric_a.xlsx"
)

################################################################################################################################################################
## METRIC B ##

class(site1_metric_b$period)
class(site1_metric_b$performance_score)

site1_metric_b <- site1_metric_b[order(as.numeric(gsub("WK", "", site1_metric_b$period))), ]

site1_metric_b_wide <- site1_metric_b %>%
  filter(warehouse_id %in% c(
    "site_1",  "site_2",  "site_3",  "site_4",  "site_5",  "site_6",
    "site_7",  "site_8",  "site_9",  "site_10", "site_11", "site_12",
    "site_13", "site_14", "site_15", "site_16", "site_17", "site_18",
    "site_19", "site_20", "site_21", "site_22", "site_23", "site_24",
    "site_25", "site_26"
  )) %>%
  mutate(performance_score = as.numeric(performance_score)) %>%
  filter(!is.na(performance_score), !is.na(period)) %>%
  select(warehouse_id, period, performance_score) %>%
  pivot_wider(
    names_from  = period,
    values_from = performance_score,
    values_fill = 0,
    values_fn   = sum
  )

site1_metric_b_total <- site1_metric_b %>%
  filter(warehouse_id %in% c(
    "site_1",  "site_2",  "site_3",  "site_4",  "site_5",  "site_6",
    "site_7",  "site_8",  "site_9",  "site_10", "site_11", "site_12",
    "site_13", "site_14", "site_15", "site_16", "site_17", "site_18",
    "site_19", "site_20", "site_21", "site_22", "site_23", "site_24",
    "site_25", "site_26"
  )) %>%
  group_by(warehouse_id) %>%
  summarise(total_score = sum(performance_score, na.rm = TRUE))

site1_metric_b_total <- site1_metric_b_total[order(as.numeric(site1_metric_b_total$total_score)), ]

View(site1_metric_b_wide)
View(site1_metric_b_total)

write.xlsx(
  list(
    dashboard = site1_metric_b,
    wide      = site1_metric_b_wide,
    total     = site1_metric_b_total
  ),
  file = "site1_metric_b.xlsx"
)

################################################################################################################################################################
## METRIC C ##

site1_metric_c <- site1_metric_c %>%
  separate(record_date, into = c("date", "time"), sep = "T")

site1_metric_c$date   <- ymd(site1_metric_c$date)
site1_metric_c$wk_num <- as.numeric(format(site1_metric_c$date, "%U")) + 1
site1_metric_c        <- site1_metric_c %>% filter(wk_num != 35)

site1_metric_c_total <- site1_metric_c %>%
  group_by(fc) %>%
  summarise(missed_count = sum(missed_count, na.rm = TRUE))

site1_metric_c_wide <- site1_metric_c %>%
  mutate(missed = as.numeric(missed_count)) %>%
  filter(!is.na(missed_count), !is.na(wk_num)) %>%
  select(fc, wk_num, missed_count) %>%
  pivot_wider(
    names_from  = wk_num,
    values_from = missed_count,
    values_fill = 0,
    values_fn   = sum
  )

site1_metric_c_wide <- site1_metric_c_wide %>%
  select(fc, all_of(as.character(sort(as.integer(setdiff(names(.), "fc"))))))

site1_metric_c_total <- site1_metric_c_total[order(as.numeric(site1_metric_c_total$missed_count)), ]

View(site1_metric_c_wide)
View(site1_metric_c_total)

write.xlsx(
  list(
    dashboard = site1_metric_c,
    wide      = site1_metric_c_wide,
    total     = site1_metric_c_total
  ),
  file = "site1_metric_c.xlsx"
)

################################################################################################################################################################
## METRIC D ##

site1_metric_d$ship_date <- as.Date(site1_metric_d$ship_date, origin = "1899-12-30")
site1_metric_d$wk_num    <- as.numeric(format(site1_metric_d$ship_date, "%U")) + 1
site1_metric_d           <- site1_metric_d[-c(4)]

site1_metric_d_wide <- site1_metric_d %>%
  mutate(missed_units = as.numeric(units)) %>%
  filter(!is.na(units), !is.na(wk_num)) %>%
  select(warehouse_id, wk_num, units) %>%
  pivot_wider(
    names_from  = wk_num,
    values_from = units,
    values_fill = 0,
    values_fn   = sum
  )

site1_metric_d_wide_filtered <- site1_metric_d_wide %>%
  filter(warehouse_id %in% c(
    "site_1",  "site_2",  "site_3",  "site_4",  "site_5",  "site_6",
    "site_7",  "site_8",  "site_9",  "site_10", "site_11", "site_12",
    "site_13", "site_14", "site_15", "site_16", "site_17", "site_18",
    "site_19", "site_20", "site_21", "site_22", "site_23", "site_24",
    "site_25", "site_26", "site_27"
  )) %>%
  select(warehouse_id, all_of(as.character(sort(as.integer(setdiff(names(.), "warehouse_id"))))))

site1_metric_d_total <- site1_metric_d %>%
  filter(warehouse_id %in% c(
    "site_1",  "site_2",  "site_3",  "site_4",  "site_5",  "site_6",
    "site_7",  "site_8",  "site_9",  "site_10", "site_11", "site_12",
    "site_13", "site_14", "site_15", "site_16", "site_17", "site_18",
    "site_19", "site_20", "site_21", "site_22", "site_23", "site_24",
    "site_25", "site_26"
  )) %>%
  group_by(warehouse_id) %>%
  summarise(units = sum(units, na.rm = TRUE)) %>%
  arrange(desc(units))

View(site1_metric_d_wide_filtered)
View(site1_metric_d_total)

write.xlsx(
  list(
    dashboard = site1_metric_d,
    wide      = site1_metric_d_wide,
    total     = site1_metric_d_total
  ),
  file = "site1_metric_d.xlsx"
)

################################################################################################################################################################
## METRIC E ##

View(site1_metric_e)

unique(site1_metric_e$dow)

dow_levels <- c("SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT")
site1_metric_e$dow <- factor(site1_metric_e$dow, levels = dow_levels)

################################################################################################################################################################
## VISUALISATION ##
################################################################################################################################################################
## Metric A Root Cause Chart ##

site1_metric_a_ttl_rc <- site1_metric_a %>%
  group_by(issue_category) %>%
  summarise(score = sum(variance_score, na.rm = TRUE))

View(site1_metric_a_ttl_rc)

root_cause_chart <- ggplot(data = site1_metric_a_ttl_rc) +
  geom_col(
    mapping = aes(x = issue_category, y = score),
    fill    = "#86b7f0",
    colour  = "black",
    width   = 0.45
  ) +
  geom_text(
    mapping = aes(x = issue_category, y = score, label = score),
    vjust   = -0.5,
    size    = 8
  ) +
  theme_dark() +
  theme(
    plot.background = element_rect(fill = "grey", colour = "black"),
    plot.title      = element_text(size = 22),
    axis.text.x     = element_text(angle = 45, hjust = 1, size = 15),
    axis.text.y     = element_text(size = 15),
    axis.title      = element_text(size = 18)
  ) +
  labs(
    title = "Issue Category by Performance Score",
    x     = "Issue Category",
    y     = "Performance Score"
  )

root_cause_chart
ggsave("root_cause_chart.png", root_cause_chart, width = 12, height = 8, dpi = 300)

################################################################################################################################################################
## Metric C WoW Graph ##

site1_metric_c_graph <- site1_metric_c %>%
  filter(fc == "site_1") %>%
  group_by(wk_num) %>%
  summarise(missed_count = sum(missed_count, na.rm = TRUE))

metric_c_graph <- ggplot(data = site1_metric_c_graph, aes(x = wk_num, y = missed_count, fill = wk_num)) +
  geom_col(alpha = 0.7) +
  theme_dark() +
  theme(
    panel.background = element_rect(colour = "black"),
    plot.title       = element_text(size = 22, hjust = 0.5),
    axis.text.x      = element_text(angle = 45, hjust = 1, size = 15),
    axis.text.y      = element_text(size = 15),
    axis.title       = element_text(size = 18)
  ) +
  geom_text(aes(label = missed_count), vjust = -0.5, size = 5) +
  scale_x_continuous(breaks = 0:36) +
  labs(
    x     = "Week Number",
    y     = "Missed Count",
    title = "site_1 Metric C WoW"
  )

metric_c_graph
ggsave("metric_c_graph.png", metric_c_graph, width = 12, height = 8, dpi = 300)

################################################################################################################################################################
## Metric A WoW Graph ##

site1_metric_a_graph <- site1_metric_a %>%
  filter(fc == "site_1") %>%
  group_by(week) %>%
  summarise(variance_pct = round(median(variance_pct, na.rm = TRUE), 2))

metric_a_graph <- ggplot(data = site1_metric_a_graph, aes(x = factor(week), y = variance_pct, fill = factor(week))) +
  geom_col(alpha = 0.85) +
  scale_fill_manual(values = colorRampPalette(c("#003B5C", "#0072B8", "#A8C8E8"))(length(unique(site1_metric_a_graph$week)))) +
  theme_minimal() +
  theme(
    panel.background   = element_rect(fill = "#F5F7FA", colour = NA),
    plot.background    = element_rect(fill = "#F5F7FA", colour = NA),
    panel.grid.major.y = element_line(colour = "#D0D7DE", linetype = "dashed"),
    panel.grid.major.x = element_blank(),
    panel.grid.minor   = element_blank(),
    plot.title         = element_text(size = 22, hjust = 0.5, colour = "#003B5C", face = "bold"),
    axis.text.x        = element_text(angle = 45, hjust = 1, size = 13, colour = "#333333"),
    axis.text.y        = element_text(size = 13, colour = "#333333"),
    axis.title         = element_text(size = 16, colour = "#003B5C"),
    legend.position    = "none"
  ) +
  geom_text(aes(label = paste0(variance_pct, "%")), vjust = -0.5, size = 5.5, colour = "#003B5C", fontface = "bold") +
  labs(
    x     = "Week Number",
    y     = "Variance %",
    title = "site_1 Average Metric A WoW"
  )

metric_a_graph
ggsave("metric_a_graph.png", metric_a_graph, width = 12, height = 8, dpi = 300)

################################################################################################################################################################
## Metric B WoW Graph ##

site1_metric_b_graph <- site1_metric_b %>%
  filter(warehouse_id == "site_1") %>%
  group_by(period) %>%
  summarise(score = round(sum(performance_score, na.rm = TRUE), 3))

metric_b_graph <- ggplot(data = site1_metric_b_graph, aes(x = period, y = score, fill = period)) +
  geom_col(alpha = 0.7) +
  theme_dark() +
  theme(
    panel.background = element_rect(colour = "black"),
    plot.title       = element_text(size = 22, hjust = 0.5),
    axis.text.x      = element_text(angle = 45, hjust = 1, size = 15),
    axis.text.y      = element_text(size = 15),
    axis.title       = element_text(size = 18)
  ) +
  geom_text(aes(label = score), vjust = -0.5, size = 6) +
  labs(
    x     = "Week Number",
    y     = "Performance Score",
    title = "site_1 Metric B Score WoW"
  )

metric_b_graph
ggsave("metric_b_graph.png", metric_b_graph, width = 12, height = 8, dpi = 300)

################################################################################################################################################################
## Metric B Root Cause Graph ##

site1_rc_metric_b_graph <- site1_metric_b %>%
  filter(warehouse_id == "site_1") %>%
  group_by(issue_category) %>%
  summarise(score = round(sum(performance_score, na.rm = TRUE), 2))

rc_metric_b_graph <- ggplot(data = site1_rc_metric_b_graph, aes(x = issue_category, y = score, fill = issue_category)) +
  geom_col(alpha = 0.7) +
  theme_dark() +
  theme(
    panel.background = element_rect(colour = "black"),
    plot.title       = element_text(size = 22, hjust = 0.5),
    axis.text.x      = element_text(angle = 45, hjust = 1, size = 15),
    axis.text.y      = element_text(size = 15),
    axis.title       = element_text(size = 18)
  ) +
  geom_text(aes(label = score), vjust = -0.5, size = 6) +
  labs(
    x     = "Issue Category",
    y     = "Performance Score",
    title = "site_1 Metric B Issue Category Breakdown"
  )

rc_metric_b_graph
ggsave("rc_metric_b_graph.png", rc_metric_b_graph, width = 12, height = 8, dpi = 300)

################################################################################################################################################################
## Metric D WoW Graph ##

site1_metric_d_graph <- site1_metric_d %>%
  filter(warehouse_id == "site_1") %>%
  group_by(wk_num) %>%
  summarise(units = sum(units, na.rm = TRUE))

site1_metric_d_graph <- site1_metric_d_graph[order(as.numeric(site1_metric_d_graph$wk_num)), ]

metric_d_graph <- ggplot(data = site1_metric_d_graph, aes(x = wk_num, y = units, fill = wk_num)) +
  geom_col(alpha = 0.7) +
  theme_dark() +
  theme(
    panel.background = element_rect(colour = "black"),
    plot.title       = element_text(size = 22, hjust = 0.5),
    axis.text.x      = element_text(angle = 45, hjust = 1, size = 15),
    axis.text.y      = element_text(size = 15),
    axis.title       = element_text(size = 18)
  ) +
  geom_text(aes(label = units), vjust = -0.5, size = 5) +
  scale_x_continuous(breaks = 0:36) +
  labs(
    x     = "Week Number",
    y     = "Units",
    title = "site_1 Metric D WoW"
  )

metric_d_graph
ggsave("metric_d_graph.png", metric_d_graph, width = 12, height = 8, dpi = 300)

################################################################################################################################################################
## Metric D by Date Graph ##

site1_metric_d_date <- site1_metric_d %>%
  filter(warehouse_id == "site_1") %>%
  select(ship_date, units)

site1_metric_d_date$ship_date <- ymd(site1_metric_d_date$ship_date)

metric_d_day_graph <- ggplot(data = site1_metric_d_date, aes(x = ship_date, y = units, fill = units)) +
  geom_col(alpha = 0.8) +
  scale_fill_gradient(low = "#1E90FF", high = "#FF4500") +
  scale_x_date(date_breaks = "1 day", date_labels = "%d/%m") +
  theme_dark() +
  theme(
    panel.background = element_rect(fill = "#1a1a2e", colour = "black"),
    plot.background  = element_rect(fill = "#1a1a2e"),
    plot.title       = element_text(size = 22, hjust = 0.5, colour = "white"),
    axis.text.x      = element_text(angle = 45, hjust = 1, size = 10, colour = "white"),
    axis.text.y      = element_text(size = 15, colour = "white"),
    axis.title       = element_text(size = 18, colour = "white"),
    legend.position  = "none"
  ) +
  geom_text(aes(label = units), vjust = -0.5, size = 3.5, colour = "white") +
  labs(
    x     = "Date",
    y     = "Units",
    title = "site_1 Metric D by Date"
  )

metric_d_day_graph
ggsave("metric_d_day_graph.png", metric_d_day_graph, width = 16, height = 8, dpi = 300)

################################################################################################################################################################
## Metric E Heatmap ##

metric_e_plot <- ggplot(site1_metric_e, aes(x = hour, y = dow, fill = total_score)) +
  geom_tile(colour = "white", linewidth = 0.5) +
  geom_text(aes(label = round(total_score, 2)), size = 3, colour = "white") +
  scale_x_continuous(breaks = 0:31) +
  labs(
    title = "Performance Score by Day of Week and Hour",
    x     = "Hour of Day",
    y     = "Day of Week",
    fill  = "Score"
  ) +
  theme_minimal() +
  theme(panel.grid = element_blank())

metric_e_plot
ggsave("metric_e_plot.png", metric_e_plot, width = 12, height = 8, dpi = 300)

################################################################################################################################################################
