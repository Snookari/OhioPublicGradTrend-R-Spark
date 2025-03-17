# Cài đặt và load thư viện
if (!require(sparklyr)) install.packages("sparklyr")
if (!require(dplyr)) install.packages("dplyr")
if (!require(ggplot2)) install.packages("ggplot2")
if (!require(data.table)) install.packages("data.table")
if (!require(randomForest)) install.packages("randomForest")
if (!require(cluster)) install.packages("cluster")
if (!require(factoextra)) install.packages("factoextra")
if (!require(car)) install.packages("car")

library(sparklyr)
library(dplyr)
library(ggplot2)
library(data.table)
library(randomForest)
library(cluster)
library(factoextra)

# Tạo thư mục lưu biểu đồ
if (!dir.exists("plots")) dir.create("plots")

# Khởi tạo Spark
sc <- spark_connect(master = "local", config = list(spark.driver.memory = "8G", spark.executor.memory = "8G"))

# Đọc dữ liệu bằng Spark
file_path <- "higher_ed_employee_salaries.csv"
df_spark <- spark_read_csv(sc, name = "salary_data", path = file_path, inferSchema = TRUE, header = TRUE)

# Chuyển đổi dữ liệu Spark thành R data.table
df <- df_spark %>% collect()
setDT(df)

# Đổi tên cột nếu cần
if ("Job.Description" %in% names(df)) setnames(df, "Job.Description", "Job_Description")
if ("# Earnings" %in% names(df)) setnames(df, "# Earnings", "Earnings")
if ("# Year" %in% names(df)) setnames(df, "# Year", "Year")

# Xử lý NA
mean_earnings <- mean(df$Earnings, na.rm = TRUE)
df[is.na(Earnings), Earnings := mean_earnings]
df[is.na(School), School := "Unknown"]
df[is.na(Job_Description), Job_Description := "Unknown"]
df[is.na(Department), Department := "Unknown"]

# Chuyển đổi kiểu dữ liệu
df[, Earnings := as.numeric(Earnings)]
df[, Year := as.integer(Year)]

# Lọc dữ liệu
df <- df[Earnings > 0 & Earnings < 200000]
df <- df[!is.na(Earnings) & !is.na(Year)]

# Tạo cột "Condition"
df[, Condition := ifelse(Job_Description == "Professor", "Professor", "Non-Professor")]

# Thiết lập theme ggplot2
theme_set(theme_light(base_size = 12))

# 1. Biểu đồ số lượng nhân viên theo School
plot1 <- ggplot(df, aes(x = School)) +
  geom_bar(fill = "#3399FF") +
  labs(title = "Số lượng nhân viên theo trường",
       x = "Trường", y = "Số lượng nhân viên") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
ggsave("plots/1_so_luong_nhan_vien_theo_truong.png", plot1, width = 10, height = 6)

# 2. Biểu đồ tròn tỷ lệ Professor và Non-Professor
condition_counts <- df[, .N, by = Condition]

plot2 <- ggplot(condition_counts, aes(x = "", y = N, fill = Condition)) +
  geom_bar(stat = "identity", width = 1) +
  coord_polar("y", start = 0) +
  labs(title = "Tỷ lệ nhân viên Professor và Non-Professor", fill = "Loại") +
  theme_light() +
  geom_text(aes(label = scales::percent(N / sum(N), accuracy = 0.1)), 
            position = position_stack(vjust = 0.5)) +
  theme(
    axis.title = element_blank(),
    axis.text = element_blank(),
    axis.ticks = element_blank(),
    panel.grid = element_blank()
  )

ggsave("plots/2_ty_le_professor_vs_nonprofessor.png", plot2, width = 6, height = 6)



# 3. Biểu đồ phân tán giữa Year và Earnings
plot3 <- ggplot(df, aes(x = Year, y = Earnings, color = Condition)) +
  geom_point(alpha = 0.7) +
  scale_color_manual(values = c("#ff9999", "#66b3ff")) +
  labs(title = "Earnings so với Year", x = "Năm", y = "Lương (Earnings)") +
  theme_light()
ggsave("plots/3_earnings_vs_year.png", plot3, width = 10, height = 6)

# 4. Phân cụm K-Means
# Chuẩn bị dữ liệu
X_cluster <- df[, .(Earnings, Year)]
X_scaled <- scale(X_cluster)

# Phân cụm KMeans
set.seed(123)
k <- 3
kmeans_result <- kmeans(X_scaled, centers = k, nstart = 25)

# Tạo biểu đồ phân cụm PCA
p <- fviz_cluster(kmeans_result, data = X_scaled,
                  geom = "point",
                  ellipse = TRUE,
                  show.clust.cent = TRUE,
                  palette = c("#e41a1c", "#377eb8", "#4daf4a"),
                  ggtheme = theme_light(),
                  main = "Biểu đồ phân cụm K-Means (PCA)")

# Lưu biểu đồ vào thư mục "plots"
ggsave("plots/4.phan_cum_kmeans_PCA.png", p, width = 10, height = 6, dpi = 300)

# 5. Dự đoán số lượng nhân viên theo School
df_school_trend <- df[, .N, by = School][order(-N)]
setnames(df_school_trend, "N", "Count")
X_school <- 1:nrow(df_school_trend)
y_school <- df_school_trend$Count

years_future <- (max(df$Year) + 1):(max(df$Year) + 5)
school_model <- randomForest(x = matrix(X_school, ncol = 1), y = y_school, ntree = 100)
future_school_sales <- predict(school_model, matrix(X_school, ncol = 1))

plot5 <- ggplot(df_school_trend, aes(x = School, y = Count)) +
  geom_bar(stat = "identity", fill = "#3399FF", alpha = 0.7) +
  geom_point(aes(x = School, y = future_school_sales), color = "red", shape = 4, size = 3) +
  labs(title = "Dự đoán số lượng nhân viên theo trường",
       x = "Trường", y = "Số lượng nhân viên") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
ggsave("plots/5_du_doan_so_luong_nhan_vien_theo_truong.png", plot5, width = 10, height = 6)

# 6. Dự đoán tỷ lệ Professor theo năm
df_condition_trend <- df[, .(Prop_Professor = mean(Condition == "Professor")), by = Year]
setorder(df_condition_trend, Year)
X_condition <- df_condition_trend$Year
y_condition <- df_condition_trend$Prop_Professor

condition_model <- randomForest(x = matrix(X_condition, ncol = 1), y = y_condition, ntree = 100)
future_condition_ratio <- predict(condition_model, matrix(years_future, ncol = 1))

plot6 <- ggplot() +
  geom_line(data = df_condition_trend, aes(x = Year, y = Prop_Professor), color = "black") +
  geom_point(data = df_condition_trend, aes(x = Year, y = Prop_Professor), color = "black", size = 3) +
  geom_line(data = data.frame(Year = years_future, Prop_Professor = future_condition_ratio), 
            aes(x = Year, y = Prop_Professor), color = "red", linetype = "dashed") +
  geom_point(data = data.frame(Year = years_future, Prop_Professor = future_condition_ratio), 
             aes(x = Year, y = Prop_Professor), color = "red", size = 3) +
  labs(title = "Dự đoán tỷ lệ nhân viên Professor trong tương lai",
       x = "Năm", y = "Tỷ lệ Professor") +
  theme(legend.position = "none")
ggsave("plots/6_du_doan_ty_le_professor.png", plot6, width = 10, height = 6)

# 7. Dự đoán tổng số nhân viên theo năm
df_sales_trend <- df[, .N, by = Year][order(Year)]
setnames(df_sales_trend, "N", "TotalSales")
X_sales <- df_sales_trend$Year
y_sales <- df_sales_trend$TotalSales

sales_model <- randomForest(x = matrix(X_sales, ncol = 1), y = y_sales, ntree = 100)
future_sales <- predict(sales_model, matrix(years_future, ncol = 1))

plot7 <- ggplot() +
  geom_line(data = df_sales_trend, aes(x = Year, y = TotalSales), color = "black") +
  geom_point(data = df_sales_trend, aes(x = Year, y = TotalSales), color = "black", size = 3) +
  geom_line(data = data.frame(Year = years_future, TotalSales = future_sales), 
            aes(x = Year, y = TotalSales), color = "red", linetype = "dashed") +
  geom_point(data = data.frame(Year = years_future, TotalSales = future_sales), 
             aes(x = Year, y = TotalSales), color = "red", size = 3) +
  labs(title = "Dự đoán tổng số nhân viên theo năm",
       x = "Năm", y = "Tổng số nhân viên") +
  theme(legend.position = "none")
ggsave("plots/7_du_doan_tong_so_nhan_vien.png", plot7, width = 10, height = 6)

# 8. Biểu đồ phân phối top 10 phòng ban (Departments)

# Tính số lượng theo Department, sắp xếp giảm dần, lấy top 10
top_departments <- df[, .N, by = Department][order(-N)][1:10]

# Vẽ biểu đồ
plot8 <- ggplot(top_departments, aes(x = reorder(Department, -N), y = N)) +
  geom_bar(stat = "identity", fill = "steelblue") +
  labs(title = "Phân phối Top 10 phòng ban",
       x = "Phòng ban", y = "Số lượng nhân viên") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# Lưu biểu đồ
ggsave("plots/8_phan_phoi_top_10_phong_ban.png", plot8, width = 10, height = 6)


# 9. Biểu đồ phân phối số lượng nhân viên theo 13 trường hàng đầu

# Tính số lượng nhân viên theo từng trường, sắp xếp giảm dần và lấy top 13
top_schools <- df[, .N, by = School][order(-N)][1:13]

# Vẽ biểu đồ
plot9 <- ggplot(top_schools, aes(x = reorder(School, -N), y = N)) +
  geom_bar(stat = "identity", fill = "#3399FF", alpha = 0.8) +
  labs(title = "Phân phối số lượng nhân viên tại 13 trường hàng đầu",
       x = "Trường", y = "Số lượng nhân viên") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# Lưu biểu đồ
ggsave("plots/9_phan_phoi_so_luong_nhan_vien_13_truong.png", plot9, width = 10, height = 6)

# 10. Biểu đồ phân phối số lượng nhân viên tại 5 phòng ban ít người nhất

# Tính số lượng nhân viên theo phòng ban, sắp xếp tăng dần và lấy 5 phòng ban cuối
bottom_departments <- df[, .N, by = Department][order(N)][1:5]

# Vẽ biểu đồ
plot10 <- ggplot(bottom_departments, aes(x = reorder(Department, N), y = N)) +
  geom_bar(stat = "identity", fill = "#3399FF", alpha = 0.8) +
  labs(title = "Phân phối số lượng nhân viên tại 5 phòng ban ít người nhất",
       x = "Phòng ban", y = "Số lượng nhân viên") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# Lưu biểu đồ
ggsave("plots/10_phan_phoi_5_phong_ban_it_nhan_vien.png", plot10, width = 10, height = 6)

# 11. Biểu đồ phân phối số lượng nhân viên tại 5 phòng ban lớn nhất ở The Ohio State University

# Lọc dữ liệu cho trường "The Ohio State University"
df_ohio <- df[School == "The Ohio State University"]

# Tính số lượng nhân viên theo phòng ban trong trường này và lấy 5 phòng ban nhiều nhất
top_departments_ohio <- df_ohio[, .N, by = Department][order(-N)][1:5]

# Vẽ biểu đồ
plot11 <- ggplot(top_departments_ohio, aes(x = reorder(Department, -N), y = N)) +
  geom_bar(stat = "identity", fill = "#3399FF", alpha = 0.8) +
  labs(title = "Phân phối 5 phòng ban đông nhân viên nhất tại The Ohio State University",
       x = "Phòng ban", y = "Số lượng nhân viên") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# Lưu biểu đồ
ggsave("plots/11_5_phong_ban_lon_nhat_ohio_state.png", plot11, width = 10, height = 6)

# 12. Biểu đồ mức lương trung bình theo năm tại The Ohio State University

# Lọc dữ liệu theo trường
df_ohio <- df[School == "The Ohio State University"]

# Tính lương trung bình theo từng năm
avg_earnings_per_year <- df_ohio[, .(Average_Earnings = mean(Earnings, na.rm = TRUE)), by = Year][order(Year)]

# Vẽ biểu đồ cột
plot12 <- ggplot(avg_earnings_per_year, aes(x = factor(Year), y = Average_Earnings)) +
  geom_bar(stat = "identity", fill = "#3399FF", alpha = 0.8) +
  labs(title = "Mức lương trung bình mỗi năm tại The Ohio State University",
       x = "Năm", y = "Mức lương trung bình (Earnings)") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# Lưu biểu đồ
ggsave("plots/12_muc_luong_trung_binh_theo_nam_ohio_state.png", plot12, width = 10, height = 6)

# 13. Biểu đồ violin: So sánh mức lương mỗi năm tại The Ohio State University

# Lọc dữ liệu theo trường
df_ohio <- df[School == "The Ohio State University"]

# Vẽ biểu đồ violin (dựa trên toàn bộ dữ liệu lương theo năm)
plot13 <- ggplot(df_ohio, aes(x = factor(Year), y = Earnings)) +
  geom_violin(fill = "#66CCFF", alpha = 0.8) +
  stat_summary(fun = mean, geom = "point", shape = 21, size = 3, fill = "white", color = "black") +
  labs(title = "So sánh phân bố mức lương theo năm tại The Ohio State University",
       x = "Năm", y = "Mức lương (Earnings)") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# Lưu biểu đồ
ggsave("plots/13_so_sanh_muc_luong_violin_theo_nam_ohio_state.png", plot13, width = 10, height = 6)

# Tính mức lương trung bình theo Department tại The Ohio State University
avg_earnings_per_dept <- df_ohio %>%
  group_by(Department) %>%
  summarise(`Mức lương trung bình` = mean(Earnings)) %>%
  arrange(desc(`Mức lương trung bình`)) %>%
  slice_head(n = 5)

# 14. Biểu đồ mức lương trung bình theo năm tại Shawnee State University

# Lọc dữ liệu cho trường "Shawnee State University"
df_shawnee <- df[School == "Shawnee State University"]

# Tính mức lương trung bình theo từng năm
avg_earnings_shawnee <- df_shawnee[, .(Average_Earnings = mean(Earnings, na.rm = TRUE)), by = Year][order(Year)]

# Vẽ biểu đồ cột
plot14 <- ggplot(avg_earnings_shawnee, aes(x = factor(Year), y = Average_Earnings)) +
  geom_bar(stat = "identity", fill = "#3399FF", alpha = 0.8) +
  labs(title = "Mức lương trung bình mỗi năm tại Shawnee State University",
       x = "Năm", y = "Mức lương trung bình (Earnings)") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# Lưu biểu đồ
ggsave("plots/14_muc_luong_trung_binh_theo_nam_shawnee_state.png", plot14, width = 10, height = 6)

#15. Biểu đồ violin: So sánh mức lương theo năm tại Shawnee State University
# Lọc dữ liệu cho trường "Shawnee State University"
df_shawnee <- df[School == "Shawnee State University"]

# Vẽ biểu đồ violin (dựa trên toàn bộ dữ liệu lương theo năm)
plot15 <- ggplot(df_shawnee, aes(x = factor(Year), y = Earnings)) +
  geom_violin(fill = "#3399FF", alpha = 0.8) +
  stat_summary(fun = mean, geom = "point", shape = 21, size = 3, fill = "white", color = "black") +
  labs(title = "So sánh phân bố mức lương theo năm tại Shawnee State University",
       x = "Năm", y = "Mức lương (Earnings)") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# Lưu biểu đồ
ggsave("plots/15_so_sanh_muc_luong_violin_theo_nam_shawnee_state.png", plot15, width = 10, height = 6)



# Ngắt kết nối Spark
spark_disconnect(sc)
