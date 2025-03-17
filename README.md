# XU HƯỚNG SAU TỐT NGHIỆP CỦA SINH VIÊN TRƯỜNG CÔNG TẠI BANG OHIO SỬ DỤNG NGÔN NGỮ R VÀ SPARK - DNU
---

## 1. Giới thiệu  

### Môn học Big Data  
Big Data cung cấp kiến thức nền tảng và chuyên sâu về cách thu thập, lưu trữ, xử lý và phân tích khối lượng dữ liệu cực lớn (dữ liệu lớn) trong thời gian ngắn. Sinh viên được tiếp cận với các công nghệ và công cụ phổ biến trong lĩnh vực này như **Hadoop, Spark, NoSQL**, và các hệ thống phân tán. Ngoài ra, môn học còn nhấn mạnh đến khả năng khai thác dữ liệu để tạo ra giá trị thực tiễn trong các lĩnh vực như kinh doanh, khoa học, y tế và trí tuệ nhân tạo.  
Môn học được hướng dẫn bởi hai giảng viên **Lê Thị Thùy Trang** và **Trần Quý Nam**, thuộc khoa **Công Nghệ Thông Tin**, trường **Đại học Đại Nam**.  

### Dự án XU HƯỚNG SAU TỐT NGHIỆP CỦA SINH VIÊN TRƯỜNG CÔNG TẠI BANG OHIO SỬ DỤNG NGÔN NGỮ R VÀ SPARK
Dự án này nhằm phân tích xu hướng nghề nghiệp và mức lương sau tốt nghiệp thông qua dữ liệu lương của nhân viên tại các trường đại học công lập ở bang Ohio. Bằng cách sử dụng Sparklyr để xử lý dữ liệu lớn và ggplot2 để trực quan hóa, chương trình hỗ trợ việc hiểu rõ hơn về phân bố nhân sự, mức thu nhập và các yếu tố ảnh hưởng đến cơ hội nghề nghiệp. Dự án sử dụng các kỹ thuật phân tích dữ liệu, phân cụm K-Means và dự đoán bằng Random Forest nhằm dự báo xu hướng phát triển nguồn nhân lực và hỗ trợ sinh viên, nhà trường, cũng như các nhà hoạch định chính sách đưa ra quyết định hiệu quả hơn. 

### Phân cụm K-means  
K-means là một thuật toán phân cụm không giám sát, được sử dụng để **nhóm các điểm dữ liệu vào các cụm dựa trên sự tương đồng**. Trong dự án này, K-means có thể được sử dụng để **phân nhóm các quốc gia hoặc khu vực** dựa trên các chỉ số COVID-19 như **số ca nhiễm, số ca tử vong và tỷ lệ hồi phục**.  

### Mô hình Random Forest  
Random Forest là một **mô hình học máy mạnh mẽ dựa trên các cây quyết định** (decision trees). Trong dự án này, Random Forest được sử dụng để **dự đoán số lượng nhân viên, tỷ lệ Professor và mức lương trong tương lai** dựa trên dữ liệu lịch sử. Nhờ khả năng xử lý dữ liệu phức tạp và không yêu cầu giả định về phân phối, mô hình giúp phân tích xu hướng hiệu quả và hỗ trợ việc ra quyết định chính xác hơn trong lĩnh vực nhân sự giáo dục.

### Thành viên nhóm  
Nhóm thực hiện dự án gồm 4 thành viên:  

| Họ và Tên | Mã Sinh Viên |  
|-----------|-------------|  
| Nguyễn Quang Hiệp | 1671020115 |  
| Võ Vĩnh Thái | 1671020289 |  
| Đỗ Trường Anh | 1671020015 |  
| Lê Đức Mạnh | 1671020200 |  

---

## 2. Dữ liệu
Dữ liệu được lấy từ **Kaggle**, ghi lại thông tin về **nhân sự và tiền lương** trong các trường đại học Mỹ. Tập dữ liệu bao gồm thông tin như:

- **School (Trường học)**
- **Department (Phòng ban)**
- **Job Description (Chức danh công việc)**
- **Earnings (Mức lương)**
- **Year (Năm)**
Lưu ý khi xử lý dữ liệu:
- Các cột **"Job.Description", "# Earnings", "# Year"** cần đổi tên, chuẩn hóa kiểu dữ liệu.
- Các giá trị thiếu (NA) trong cột lương được thay thế bằng **giá trị trung bình**.
- Dữ liệu được lọc để loại bỏ các giá trị bất hợp lý và chuẩn hóa cho phân tích.

## 3. Mô hình và phân tích
**Phân cụm K-Means**
- Dữ liệu được phân cụm bằng K-Means dựa trên Earnings và Year để xác định nhóm nhân viên theo mức lương và thời gian làm việc.

## Dự đoán với Random Forest
Mô hình Random Forest được sử dụng để:
- **Dự đoán số lượng nhân viên theo trường**.
- **Dự đoán tỷ lệ giáo sư (Professor) qua các năm**.
- **Dự đoán tổng số nhân viên trong tương lai**.

## 4. Trực quan hóa
Dự án tạo ra nhiều **biểu đồ phân tích** như:
- Biểu đồ cột và tròn: phân bố nhân viên theo trường, phòng ban, chức danh.
- Biểu đồ phân tán và violin: phân tích mức lương theo thời gian.
- Biểu đồ phân cụm PCA.
- Biểu đồ dự đoán xu hướng.
Tất cả biểu đồ được lưu vào thư mục **plots/** sau khi chạy chương trình.

---

## 5. Cài đặt

### 5.1. Cài đặt thư viện

Chạy lệnh sau để cài đặt các thư viện cần thiết nếu chưa có:

```r
if (!require(sparklyr)) install.packages("sparklyr")
if (!require(dplyr)) install.packages("dplyr")
if (!require(ggplot2)) install.packages("ggplot2")
if (!require(data.table)) install.packages("data.table")
if (!require(randomForest)) install.packages("randomForest")
if (!require(cluster)) install.packages("cluster")
if (!require(factoextra)) install.packages("factoextra")
if (!require(car)) install.packages("car")
```

### 5.2. Kết nối Spark và chạy chương trình

```r
sc <- spark_connect(master = "local")

```

## 5.3 Nạp và Tiền xử lý dữ liệu
Dữ liệu được đọc từ file higher_ed_employee_salaries.csv bằng Spark, sau đó chuyển về R để xử lý và phân tích:

- Đọc dữ liệu và chuyển đổi
```r
# Đọc dữ liệu từ CSV bằng Spark
file_path <- "higher_ed_employee_salaries.csv"
df_spark <- spark_read_csv(sc, name = "salary_data", path = file_path, inferSchema = TRUE, header = TRUE)

# Chuyển dữ liệu từ Spark về R
df <- df_spark %>% collect()
setDT(df)  # chuyển thành data.table để xử lý nhanh
```

- Xử lý cột và dữ liệu thiếu
```r
# Đổi tên cột để dễ xử lý
if ("Job.Description" %in% names(df)) setnames(df, "Job.Description", "Job_Description")
if ("# Earnings" %in% names(df)) setnames(df, "# Earnings", "Earnings")
if ("# Year" %in% names(df)) setnames(df, "# Year", "Year")

# Thay NA trong cột Earnings bằng giá trị trung bình
mean_earnings <- mean(df$Earnings, na.rm = TRUE)
df[is.na(Earnings), Earnings := mean_earnings]

# Thay NA trong các cột văn bản bằng "Unknown"
df[is.na(School), School := "Unknown"]
df[is.na(Job_Description), Job_Description := "Unknown"]
df[is.na(Department), Department := "Unknown"]
```
- Chuyển kiểu dữ liệu và lọc hợp lệ
```r
# Chuyển kiểu dữ liệu
df[, Earnings := as.numeric(Earnings)]
df[, Year := as.integer(Year)]

# Lọc dữ liệu lương hợp lý và loại bỏ NA còn lại
df <- df[Earnings > 0 & Earnings < 200000]
df <- df[!is.na(Earnings) & !is.na(Year)]
```

- Tạo cột mới phân loại công việc
```r
# Phân loại công việc thành Professor hoặc Non-Professor
df[, Condition := ifelse(Job_Description == "Professor", "Professor", "Non-Professor")]
```

- Thiết lập theme cho biểu đồ
```r
theme_set(theme_light(base_size = 12))
```

### 6. Phân tích và Trực quan hóa Dữ liệu
## 6.1. Số lượng nhân viên theo trường
```r
plot1 <- ggplot(df, aes(x = School)) +
  geom_bar(fill = "#3399FF") +
  labs(title = "Số lượng nhân viên theo trường", x = "Trường", y = "Số lượng nhân viên") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
ggsave("plots/1_so_luong_nhan_vien_theo_truong.png", plot1, width = 10, height = 6)
```
![image](https://private-user-images.githubusercontent.com/134117986/423454260-39d5616d-6e1b-4624-ae86-deea390f679e.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3NDIyMTM0NDUsIm5iZiI6MTc0MjIxMzE0NSwicGF0aCI6Ii8xMzQxMTc5ODYvNDIzNDU0MjYwLTM5ZDU2MTZkLTZlMWItNDYyNC1hZTg2LWRlZWEzOTBmNjc5ZS5wbmc_WC1BbXotQWxnb3JpdGhtPUFXUzQtSE1BQy1TSEEyNTYmWC1BbXotQ3JlZGVudGlhbD1BS0lBVkNPRFlMU0E1M1BRSzRaQSUyRjIwMjUwMzE3JTJGdXMtZWFzdC0xJTJGczMlMkZhd3M0X3JlcXVlc3QmWC1BbXotRGF0ZT0yMDI1MDMxN1QxMjA1NDVaJlgtQW16LUV4cGlyZXM9MzAwJlgtQW16LVNpZ25hdHVyZT1hMjAwOTMzNTA3ZjQ3ZGJhMzgzNzdmOWQ3NTY4NDQ4OWNlZWRmZWM5ZjNhZDY0ZjUwMWQxNjljNDlkMDhhNWY3JlgtQW16LVNpZ25lZEhlYWRlcnM9aG9zdCJ9.xhmY3X54_vXmFU1hAv9C7ha6loXdxsBv949LJTJNN3k)

## 6.2. Tỷ lệ Professor và Non-Professor
```r
condition_counts <- df[, .N, by = Condition]
plot2 <- ggplot(condition_counts, aes(x = "", y = N, fill = Condition)) +
  geom_bar(stat = "identity", width = 1) + coord_polar("y") +
  geom_text(aes(label = scales::percent(N / sum(N), accuracy = 0.1)), position = position_stack(vjust = 0.5)) +
  labs(title = "Tỷ lệ nhân viên Professor và Non-Professor", fill = "Loại") +
  theme_void()
ggsave("plots/2_ty_le_professor_vs_nonprofessor.png", plot2, width = 6, height = 6)
```
![image](https://private-user-images.githubusercontent.com/134117986/423454430-20f9858f-46e6-498f-ad9b-912d492c80b4.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3NDIyMTM1MTMsIm5iZiI6MTc0MjIxMzIxMywicGF0aCI6Ii8xMzQxMTc5ODYvNDIzNDU0NDMwLTIwZjk4NThmLTQ2ZTYtNDk4Zi1hZDliLTkxMmQ0OTJjODBiNC5wbmc_WC1BbXotQWxnb3JpdGhtPUFXUzQtSE1BQy1TSEEyNTYmWC1BbXotQ3JlZGVudGlhbD1BS0lBVkNPRFlMU0E1M1BRSzRaQSUyRjIwMjUwMzE3JTJGdXMtZWFzdC0xJTJGczMlMkZhd3M0X3JlcXVlc3QmWC1BbXotRGF0ZT0yMDI1MDMxN1QxMjA2NTNaJlgtQW16LUV4cGlyZXM9MzAwJlgtQW16LVNpZ25hdHVyZT03M2M1YTBlMzNhZjQ5Y2JlNGZhZDEzYmFmYWUzMzlhNWY4NzBjZmY1NDkwZWE3YjhkYmM3ZTM4OGRmMDllNTA2JlgtQW16LVNpZ25lZEhlYWRlcnM9aG9zdCJ9.TghmHuyLgwzLrNxAmzdWktad30MVEXUK5hVxARqe1B8)

## 6.3. Earnings theo năm

```r
plot3 <- ggplot(df, aes(x = Year, y = Earnings, color = Condition)) +
  geom_point(alpha = 0.7) +
  labs(title = "Earnings theo Năm", x = "Năm", y = "Lương (Earnings)") +
  theme_light()
ggsave("plots/3_earnings_vs_year.png", plot3, width = 10, height = 6)
```
![image](https://private-user-images.githubusercontent.com/134117986/423455768-a3beae83-aad6-4158-b6e8-5d34821e8005.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3NDIyMTM1MjcsIm5iZiI6MTc0MjIxMzIyNywicGF0aCI6Ii8xMzQxMTc5ODYvNDIzNDU1NzY4LWEzYmVhZTgzLWFhZDYtNDE1OC1iNmU4LTVkMzQ4MjFlODAwNS5wbmc_WC1BbXotQWxnb3JpdGhtPUFXUzQtSE1BQy1TSEEyNTYmWC1BbXotQ3JlZGVudGlhbD1BS0lBVkNPRFlMU0E1M1BRSzRaQSUyRjIwMjUwMzE3JTJGdXMtZWFzdC0xJTJGczMlMkZhd3M0X3JlcXVlc3QmWC1BbXotRGF0ZT0yMDI1MDMxN1QxMjA3MDdaJlgtQW16LUV4cGlyZXM9MzAwJlgtQW16LVNpZ25hdHVyZT1kNTIyZGM0NzMzNjljMTFjZjY5NmY2OWYzNThkZjRjZjhhZmQ0Zjc4ZjA5YjE3ZGVlOGUxNDlhOWU2YzU4NDI0JlgtQW16LVNpZ25lZEhlYWRlcnM9aG9zdCJ9.3Mk0rTfJz5t4rWHvMuA_-89QRFTYwAnPw7y_VLg4sj0)

## 6.4. Phân cụm K-Means

```r
X_cluster <- df[, .(Earnings, Year)]
X_scaled <- scale(X_cluster)
kmeans_result <- kmeans(X_scaled, centers = 3, nstart = 25)

p <- fviz_cluster(kmeans_result, data = X_scaled, geom = "point", ellipse = TRUE,
                  palette = c("#e41a1c", "#377eb8", "#4daf4a"), ggtheme = theme_light(),
                  main = "Biểu đồ phân cụm K-Means (PCA)")
ggsave("plots/4.phan_cum_kmeans_PCA.png", p, width = 10, height = 6)
```
![image](https://private-user-images.githubusercontent.com/134117986/423455823-4aa7b46f-55be-4061-9190-22fef9621f9e.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3NDIyMTM1NDAsIm5iZiI6MTc0MjIxMzI0MCwicGF0aCI6Ii8xMzQxMTc5ODYvNDIzNDU1ODIzLTRhYTdiNDZmLTU1YmUtNDA2MS05MTkwLTIyZmVmOTYyMWY5ZS5wbmc_WC1BbXotQWxnb3JpdGhtPUFXUzQtSE1BQy1TSEEyNTYmWC1BbXotQ3JlZGVudGlhbD1BS0lBVkNPRFlMU0E1M1BRSzRaQSUyRjIwMjUwMzE3JTJGdXMtZWFzdC0xJTJGczMlMkZhd3M0X3JlcXVlc3QmWC1BbXotRGF0ZT0yMDI1MDMxN1QxMjA3MjBaJlgtQW16LUV4cGlyZXM9MzAwJlgtQW16LVNpZ25hdHVyZT03NTg3MjQzODhjZWY0NjA1MDA5MGFiNTliZjI0NDMyOWMzMGE4MjQ2MGNjYTNmYWI3ZDVlMjgzZDk2OGE0ZGJmJlgtQW16LVNpZ25lZEhlYWRlcnM9aG9zdCJ9.qfyZJL-NgwNblXSo5Jo20G1cpj4Yq9UxRJRyIgH-7Ow)

### 6.5. Dự đoán số nhân viên theo trường

```r
df_school_trend <- df[, .N, by = School][order(-N)]
X_school <- 1:nrow(df_school_trend)
y_school <- df_school_trend$N
school_model <- randomForest(x = matrix(X_school, ncol = 1), y = y_school, ntree = 100)
future_school_sales <- predict(school_model, matrix(X_school, ncol = 1))

plot5 <- ggplot(df_school_trend, aes(x = School, y = N)) +
  geom_bar(stat = "identity", fill = "#3399FF", alpha = 0.7) +
  geom_point(aes(y = future_school_sales), color = "red", shape = 4, size = 3) +
  labs(title = "Dự đoán số lượng nhân viên theo trường", x = "Trường", y = "Số lượng nhân viên") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
ggsave("plots/5_du_doan_so_luong_nhan_vien_theo_truong.png", plot5, width = 10, height = 6)
```
![image](https://private-user-images.githubusercontent.com/134117986/423455883-f27bb91b-3d33-4c6e-959d-aee3f43164fe.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3NDIyMTM1MzgsIm5iZiI6MTc0MjIxMzIzOCwicGF0aCI6Ii8xMzQxMTc5ODYvNDIzNDU1ODgzLWYyN2JiOTFiLTNkMzMtNGM2ZS05NTlkLWFlZTNmNDMxNjRmZS5wbmc_WC1BbXotQWxnb3JpdGhtPUFXUzQtSE1BQy1TSEEyNTYmWC1BbXotQ3JlZGVudGlhbD1BS0lBVkNPRFlMU0E1M1BRSzRaQSUyRjIwMjUwMzE3JTJGdXMtZWFzdC0xJTJGczMlMkZhd3M0X3JlcXVlc3QmWC1BbXotRGF0ZT0yMDI1MDMxN1QxMjA3MThaJlgtQW16LUV4cGlyZXM9MzAwJlgtQW16LVNpZ25hdHVyZT00MWJjOWNiYWU3NWI1OTk1OGZjMmI4MTRiM2VjMzc4NzE0MjZiN2Q4NDY0MDc4YjcyMjViNzM5OWUyNmNhYTE1JlgtQW16LVNpZ25lZEhlYWRlcnM9aG9zdCJ9.TVfEwc0H20KFlzOFvuVgNk0JCWbSL6ws3eu3VC9Y9co)

## 7. Dự đoán và phân tích xu hướng

### 7.1. Dự đoán tỷ lệ Professor

```r
df_condition_trend <- df[, .(Prop_Professor = mean(Condition == "Professor")), by = Year]
condition_model <- randomForest(x = matrix(df_condition_trend$Year, ncol = 1),
                                y = df_condition_trend$Prop_Professor, ntree = 100)
years_future <- (max(df$Year) + 1):(max(df$Year) + 5)
future_condition_ratio <- predict(condition_model, matrix(years_future, ncol = 1))

plot6 <- ggplot() +
  geom_line(data = df_condition_trend, aes(x = Year, y = Prop_Professor), color = "black") +
  geom_point(data = df_condition_trend, aes(x = Year, y = Prop_Professor), size = 3) +
  geom_line(data = data.frame(Year = years_future, Prop_Professor = future_condition_ratio),
            aes(x = Year, y = Prop_Professor), color = "red", linetype = "dashed") +
  labs(title = "Dự đoán tỷ lệ nhân viên Professor", x = "Năm", y = "Tỷ lệ Professor")
ggsave("plots/6_du_doan_ty_le_professor.png", plot6, width = 10, height = 6)
```
![image](https://private-user-images.githubusercontent.com/134117986/423455972-8ae73500-b369-45dc-809c-799b71faa566.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3NDIyMTM1ODMsIm5iZiI6MTc0MjIxMzI4MywicGF0aCI6Ii8xMzQxMTc5ODYvNDIzNDU1OTcyLThhZTczNTAwLWIzNjktNDVkYy04MDljLTc5OWI3MWZhYTU2Ni5wbmc_WC1BbXotQWxnb3JpdGhtPUFXUzQtSE1BQy1TSEEyNTYmWC1BbXotQ3JlZGVudGlhbD1BS0lBVkNPRFlMU0E1M1BRSzRaQSUyRjIwMjUwMzE3JTJGdXMtZWFzdC0xJTJGczMlMkZhd3M0X3JlcXVlc3QmWC1BbXotRGF0ZT0yMDI1MDMxN1QxMjA4MDNaJlgtQW16LUV4cGlyZXM9MzAwJlgtQW16LVNpZ25hdHVyZT0xMzRkODYzZWNmZjFjNjczNjM4YjU4YmU3NjQ3MmVlOTY2YTA5YTc0OWNjYjM4MTI0YWNkMWU2MzY5ODJjNDQyJlgtQW16LVNpZ25lZEhlYWRlcnM9aG9zdCJ9.SXzP7sUCqkpWUlp8dWsc4tCVWsBI6Q78qfN_yned-w4)

### 7.2. Dự đoán tổng số nhân viên theo năm

```r
df_sales_trend <- df[, .N, by = Year][order(Year)]
sales_model <- randomForest(x = matrix(df_sales_trend$Year, ncol = 1),
                            y = df_sales_trend$N, ntree = 100)
future_sales <- predict(sales_model, matrix(years_future, ncol = 1))

plot7 <- ggplot() +
  geom_line(data = df_sales_trend, aes(x = Year, y = N), color = "black") +
  geom_point(data = df_sales_trend, aes(x = Year, y = N), size = 3) +
  geom_line(data = data.frame(Year = years_future, N = future_sales),
            aes(x = Year, y = N), color = "red", linetype = "dashed") +
  labs(title = "Dự đoán tổng số nhân viên theo năm", x = "Năm", y = "Số nhân viên")
ggsave("plots/7_du_doan_tong_so_nhan_vien.png", plot7, width = 10, height = 6)
```
![image](https://private-user-images.githubusercontent.com/134117986/423456031-11f49204-3d94-423b-9b20-4baa9af74806.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3NDIyMTM2MDEsIm5iZiI6MTc0MjIxMzMwMSwicGF0aCI6Ii8xMzQxMTc5ODYvNDIzNDU2MDMxLTExZjQ5MjA0LTNkOTQtNDIzYi05YjIwLTRiYWE5YWY3NDgwNi5wbmc_WC1BbXotQWxnb3JpdGhtPUFXUzQtSE1BQy1TSEEyNTYmWC1BbXotQ3JlZGVudGlhbD1BS0lBVkNPRFlMU0E1M1BRSzRaQSUyRjIwMjUwMzE3JTJGdXMtZWFzdC0xJTJGczMlMkZhd3M0X3JlcXVlc3QmWC1BbXotRGF0ZT0yMDI1MDMxN1QxMjA4MjFaJlgtQW16LUV4cGlyZXM9MzAwJlgtQW16LVNpZ25hdHVyZT02NTExZmYwYzg2YTA5ZDNjODAyYjdiMzZhYzE2YzczMWNjM2VmYTg4ODI5Y2NlYjdiZjg1NTRmNTE1ODgwZTVjJlgtQW16LVNpZ25lZEhlYWRlcnM9aG9zdCJ9.pm0anLY6Kq9LykhSTSLgjZPPvnSRo2690UIPe-uxCDY)

## 8. Phân tích theo phòng ban và trường

## 8.1. Top 10 phòng ban đông nhất

```r
top_departments <- df[, .N, by = Department][order(-N)][1:10]
plot8 <- ggplot(top_departments, aes(x = reorder(Department, -N), y = N)) +
  geom_bar(stat = "identity", fill = "steelblue") +
  labs(title = "Top 10 phòng ban đông nhất", x = "Phòng ban", y = "Số lượng") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
ggsave("plots/8_phan_phoi_top_10_phong_ban.png", plot8, width = 10, height = 6)
```
![image](https://private-user-images.githubusercontent.com/134117986/423456088-7babe4af-d5c6-49f8-b284-bc8d8a680912.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3NDIyMTM2MTUsIm5iZiI6MTc0MjIxMzMxNSwicGF0aCI6Ii8xMzQxMTc5ODYvNDIzNDU2MDg4LTdiYWJlNGFmLWQ1YzYtNDlmOC1iMjg0LWJjOGQ4YTY4MDkxMi5wbmc_WC1BbXotQWxnb3JpdGhtPUFXUzQtSE1BQy1TSEEyNTYmWC1BbXotQ3JlZGVudGlhbD1BS0lBVkNPRFlMU0E1M1BRSzRaQSUyRjIwMjUwMzE3JTJGdXMtZWFzdC0xJTJGczMlMkZhd3M0X3JlcXVlc3QmWC1BbXotRGF0ZT0yMDI1MDMxN1QxMjA4MzVaJlgtQW16LUV4cGlyZXM9MzAwJlgtQW16LVNpZ25hdHVyZT04MmFjMmRhZWI2ZGIyOWJhNDBmNWE0NWQ2ODU1NjA3ZTUzOGI2ZDFmYjRmYmI3ZDg5MWQzNDEyYTRkNDlhYzUwJlgtQW16LVNpZ25lZEhlYWRlcnM9aG9zdCJ9.C16_tKI-469oUQUcridFmmGC6yceJy_fcvomyj8ga24)

### 9. Phân tích theo trường học

## 9.1. Số lượng nhân viên tại 13 trường hàng đầu

```r
top_schools <- df[, .N, by = School][order(-N)][1:13]
plot9 <- ggplot(top_schools, aes(x = reorder(School, -N), y = N)) +
  geom_bar(stat = "identity", fill = "#3399FF", alpha = 0.8) +
  labs(title = "Số lượng nhân viên tại 13 trường hàng đầu", x = "Trường", y = "Số lượng") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
ggsave("plots/9_phan_phoi_so_luong_nhan_vien_13_truong.png", plot9, width = 10, height = 6)
```

![image](https://private-user-images.githubusercontent.com/134117986/423456182-85edf9dc-ec72-4825-8762-ff3739da9ef5.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3NDIyMTM2MzYsIm5iZiI6MTc0MjIxMzMzNiwicGF0aCI6Ii8xMzQxMTc5ODYvNDIzNDU2MTgyLTg1ZWRmOWRjLWVjNzItNDgyNS04NzYyLWZmMzczOWRhOWVmNS5wbmc_WC1BbXotQWxnb3JpdGhtPUFXUzQtSE1BQy1TSEEyNTYmWC1BbXotQ3JlZGVudGlhbD1BS0lBVkNPRFlMU0E1M1BRSzRaQSUyRjIwMjUwMzE3JTJGdXMtZWFzdC0xJTJGczMlMkZhd3M0X3JlcXVlc3QmWC1BbXotRGF0ZT0yMDI1MDMxN1QxMjA4NTZaJlgtQW16LUV4cGlyZXM9MzAwJlgtQW16LVNpZ25hdHVyZT1lMjJkOTc4MDRiZTU2OTliNDYzYzlmYTAxMzMwMDljZTI1ODQ3MzkxNjJjY2I4ZTU5MWNhMjAyYjBkZWE1NDg5JlgtQW16LVNpZ25lZEhlYWRlcnM9aG9zdCJ9.wGeSNzpvqo8-AwOhvkXnv2sPFFRm1pg8roRAJyS1pXU)

### 9.2. 5 phòng ban ít người nhất

```r
bottom_departments <- df[, .N, by = Department][order(N)][1:5]
plot10 <- ggplot(bottom_departments, aes(x = reorder(Department, N), y = N)) +
  geom_bar(stat = "identity", fill = "#3399FF", alpha = 0.8) +
  labs(title = "5 phòng ban ít người nhất", x = "Phòng ban", y = "Số lượng") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
ggsave("plots/10_phan_phoi_5_phong_ban_it_nhan_vien.png", plot10, width = 10, height = 6)
```
![image](https://private-user-images.githubusercontent.com/134117986/423456270-6ec776b3-e5e5-479e-a0e4-dd174d14e459.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3NDIyMTM2NTEsIm5iZiI6MTc0MjIxMzM1MSwicGF0aCI6Ii8xMzQxMTc5ODYvNDIzNDU2MjcwLTZlYzc3NmIzLWU1ZTUtNDc5ZS1hMGU0LWRkMTc0ZDE0ZTQ1OS5wbmc_WC1BbXotQWxnb3JpdGhtPUFXUzQtSE1BQy1TSEEyNTYmWC1BbXotQ3JlZGVudGlhbD1BS0lBVkNPRFlMU0E1M1BRSzRaQSUyRjIwMjUwMzE3JTJGdXMtZWFzdC0xJTJGczMlMkZhd3M0X3JlcXVlc3QmWC1BbXotRGF0ZT0yMDI1MDMxN1QxMjA5MTFaJlgtQW16LUV4cGlyZXM9MzAwJlgtQW16LVNpZ25hdHVyZT1hNjI1NzBhMTRmYjAzMWE2NDM1ZGM1MjFlZDczYzA2Mzg0MzFjNGNhYzliY2JiOGIyMmNiNjkwNWNhYjhhYzI5JlgtQW16LVNpZ25lZEhlYWRlcnM9aG9zdCJ9.JRpYgz0vvC1ZXUw_NKJoid9rbBdbvLR1EDgcPcfQ3-M)

## 10. Phân tích theo từng trường cụ thể

## 10.1. 5 phòng ban lớn nhất tại The Ohio State University

```r
df_ohio <- df[School == "The Ohio State University"]
top_departments_ohio <- df_ohio[, .N, by = Department][order(-N)][1:5]
plot11 <- ggplot(top_departments_ohio, aes(x = reorder(Department, -N), y = N)) +
  geom_bar(stat = "identity", fill = "#3399FF", alpha = 0.8) +
  labs(title = "Top 5 phòng ban lớn nhất tại The Ohio State University", x = "Phòng ban", y = "Số lượng") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
ggsave("plots/11_5_phong_ban_lon_nhat_ohio_state.png", plot11, width = 10, height = 6)
```
![image](https://private-user-images.githubusercontent.com/134117986/423456362-e2802151-2e82-4cfa-b532-545db5645d05.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3NDIyMTM2NjQsIm5iZiI6MTc0MjIxMzM2NCwicGF0aCI6Ii8xMzQxMTc5ODYvNDIzNDU2MzYyLWUyODAyMTUxLTJlODItNGNmYS1iNTMyLTU0NWRiNTY0NWQwNS5wbmc_WC1BbXotQWxnb3JpdGhtPUFXUzQtSE1BQy1TSEEyNTYmWC1BbXotQ3JlZGVudGlhbD1BS0lBVkNPRFlMU0E1M1BRSzRaQSUyRjIwMjUwMzE3JTJGdXMtZWFzdC0xJTJGczMlMkZhd3M0X3JlcXVlc3QmWC1BbXotRGF0ZT0yMDI1MDMxN1QxMjA5MjRaJlgtQW16LUV4cGlyZXM9MzAwJlgtQW16LVNpZ25hdHVyZT00MzZhOTJkNjIwNGMxMGM3Yjk4MWUwYTAzNTkyZmI0NjU0NDJkNDBkZjZmZjJhOTA2NjdlOTQxNzZkOTkzOTE4JlgtQW16LVNpZ25lZEhlYWRlcnM9aG9zdCJ9.LplXlwdNggTi25elQ9Y3SmBbPkxsGM77BbxJJSQ-RPo)

### 10.2. Lương trung bình theo năm tại The Ohio State University

```r
avg_earnings_per_year <- df_ohio[, .(Average_Earnings = mean(Earnings, na.rm = TRUE)), by = Year][order(Year)]
plot12 <- ggplot(avg_earnings_per_year, aes(x = factor(Year), y = Average_Earnings)) +
  geom_bar(stat = "identity", fill = "#3399FF", alpha = 0.8) +
  labs(title = "Lương trung bình theo năm tại The Ohio State University", x = "Năm", y = "Lương trung bình") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
ggsave("plots/12_muc_luong_trung_binh_theo_nam_ohio_state.png", plot12, width = 10, height = 6)
```
![image](https://private-user-images.githubusercontent.com/134117986/423456459-1259a2e3-c741-4615-be17-8b2f375edb48.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3NDIyMTM2NzYsIm5iZiI6MTc0MjIxMzM3NiwicGF0aCI6Ii8xMzQxMTc5ODYvNDIzNDU2NDU5LTEyNTlhMmUzLWM3NDEtNDYxNS1iZTE3LThiMmYzNzVlZGI0OC5wbmc_WC1BbXotQWxnb3JpdGhtPUFXUzQtSE1BQy1TSEEyNTYmWC1BbXotQ3JlZGVudGlhbD1BS0lBVkNPRFlMU0E1M1BRSzRaQSUyRjIwMjUwMzE3JTJGdXMtZWFzdC0xJTJGczMlMkZhd3M0X3JlcXVlc3QmWC1BbXotRGF0ZT0yMDI1MDMxN1QxMjA5MzZaJlgtQW16LUV4cGlyZXM9MzAwJlgtQW16LVNpZ25hdHVyZT05YTllNjAxNWIwZGMyYTQ3Y2FkNGFjNzUxOWIzNTNjMGYwNDQxMGJiMzc2MjFkZjI5NGQyYzUwZjMwMDRmNGI4JlgtQW16LVNpZ25lZEhlYWRlcnM9aG9zdCJ9.eIm722nKxjeC8GiBiRy_DGB4S_K3kj4tw7VYSlLKB7A)

## 10.3. Phân bố lương theo năm (Violin) tại The Ohio State University

```r
plot13 <- ggplot(df_ohio, aes(x = factor(Year), y = Earnings)) +
  geom_violin(fill = "#66CCFF", alpha = 0.8) +
  stat_summary(fun = mean, geom = "point", size = 3, fill = "white", color = "black", shape = 21) +
  labs(title = "Phân bố lương theo năm tại The Ohio State University", x = "Năm", y = "Lương") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
ggsave("plots/13_so_sanh_muc_luong_violin_theo_nam_ohio_state.png", plot13, width = 10, height = 6)
```
![image](https://private-user-images.githubusercontent.com/134117986/423456542-a2a43b90-0bda-47e8-ae9a-67d8fca528dd.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3NDIyMTM3MDEsIm5iZiI6MTc0MjIxMzQwMSwicGF0aCI6Ii8xMzQxMTc5ODYvNDIzNDU2NTQyLWEyYTQzYjkwLTBiZGEtNDdlOC1hZTlhLTY3ZDhmY2E1MjhkZC5wbmc_WC1BbXotQWxnb3JpdGhtPUFXUzQtSE1BQy1TSEEyNTYmWC1BbXotQ3JlZGVudGlhbD1BS0lBVkNPRFlMU0E1M1BRSzRaQSUyRjIwMjUwMzE3JTJGdXMtZWFzdC0xJTJGczMlMkZhd3M0X3JlcXVlc3QmWC1BbXotRGF0ZT0yMDI1MDMxN1QxMjEwMDFaJlgtQW16LUV4cGlyZXM9MzAwJlgtQW16LVNpZ25hdHVyZT1kNDNjZGFkMGQ0OTc5MTcxMmUyMWRjOGIwMDUwNWJhMTRlY2VkMzA2NDI2ZWU4YjE0M2RlZDA3YTA0ZWI0MmE1JlgtQW16LVNpZ25lZEhlYWRlcnM9aG9zdCJ9.GHc9pztP2wIH7Zmv4EZmuQGD4nh4vuoURtipWCJifqM)

## 10.4. Lương trung bình theo năm tại Shawnee State University

```r
df_shawnee <- df[School == "Shawnee State University"]
avg_earnings_shawnee <- df_shawnee[, .(Average_Earnings = mean(Earnings, na.rm = TRUE)), by = Year][order(Year)]
plot14 <- ggplot(avg_earnings_shawnee, aes(x = factor(Year), y = Average_Earnings)) +
  geom_bar(stat = "identity", fill = "#3399FF", alpha = 0.8) +
  labs(title = "Lương trung bình theo năm tại Shawnee State University", x = "Năm", y = "Lương trung bình") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
ggsave("plots/14_muc_luong_trung_binh_theo_nam_shawnee_state.png", plot14, width = 10, height = 6)
```
![image](https://private-user-images.githubusercontent.com/134117986/423456669-f5cbe65c-9efa-4db7-b316-583b52bd5ae3.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3NDIyMTM3MTUsIm5iZiI6MTc0MjIxMzQxNSwicGF0aCI6Ii8xMzQxMTc5ODYvNDIzNDU2NjY5LWY1Y2JlNjVjLTllZmEtNGRiNy1iMzE2LTU4M2I1MmJkNWFlMy5wbmc_WC1BbXotQWxnb3JpdGhtPUFXUzQtSE1BQy1TSEEyNTYmWC1BbXotQ3JlZGVudGlhbD1BS0lBVkNPRFlMU0E1M1BRSzRaQSUyRjIwMjUwMzE3JTJGdXMtZWFzdC0xJTJGczMlMkZhd3M0X3JlcXVlc3QmWC1BbXotRGF0ZT0yMDI1MDMxN1QxMjEwMTVaJlgtQW16LUV4cGlyZXM9MzAwJlgtQW16LVNpZ25hdHVyZT1lMmE0NmUzZWM2M2ZiNzJiYTlhMGZmNTg5ZjY5MmZkNThiNTFkMGMwZWQwNjZiNzhhYzE4OTE1OGRiMDdiYzlhJlgtQW16LVNpZ25lZEhlYWRlcnM9aG9zdCJ9.8XFuMV93auRKZDYk45klejk9ULyJ_sWnTBZAEKhPflc)

## 10.5. Phân bố lương theo năm (Violin) tại Shawnee State University

```r
plot15 <- ggplot(df_shawnee, aes(x = factor(Year), y = Earnings)) +
  geom_violin(fill = "#3399FF", alpha = 0.8) +
  stat_summary(fun = mean, geom = "point", size = 3, fill = "white", color = "black", shape = 21) +
  labs(title = "Phân bố lương theo năm tại Shawnee State University", x = "Năm", y = "Lương") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
ggsave("plots/15_so_sanh_muc_luong_violin_theo_nam_shawnee_state.png", plot15, width = 10, height = 6)
```
![image](https://private-user-images.githubusercontent.com/134117986/423456765-9945bc11-33cf-400d-8fcc-8adfa7ba1f3b.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3NDIyMTM3ODcsIm5iZiI6MTc0MjIxMzQ4NywicGF0aCI6Ii8xMzQxMTc5ODYvNDIzNDU2NzY1LTk5NDViYzExLTMzY2YtNDAwZC04ZmNjLThhZGZhN2JhMWYzYi5wbmc_WC1BbXotQWxnb3JpdGhtPUFXUzQtSE1BQy1TSEEyNTYmWC1BbXotQ3JlZGVudGlhbD1BS0lBVkNPRFlMU0E1M1BRSzRaQSUyRjIwMjUwMzE3JTJGdXMtZWFzdC0xJTJGczMlMkZhd3M0X3JlcXVlc3QmWC1BbXotRGF0ZT0yMDI1MDMxN1QxMjExMjdaJlgtQW16LUV4cGlyZXM9MzAwJlgtQW16LVNpZ25hdHVyZT0wNDc1YTE5NmE3MjczYmIzNzFiMjczOGVjZTJjMTIxNzM4ZTU1YjkzYzEyOTg0YjJhMWFhNDRiOGJkOGY5MzZlJlgtQW16LVNpZ25lZEhlYWRlcnM9aG9zdCJ9.eFf5jTUy5ca_vqkms6cui6ljDdmsrsOah5jc5FYkAqM)

## 11. Ngắt kết nối Spark
```r
spark_disconnect(sc)
```

## 9. Kết luận

Dự án giúp phân tích và trực quan hóa dữ liệu lương trong giáo dục đại học.
Kết hợp phân cụm (K-Means) và dự đoán (Random Forest) giúp đưa ra nhận định và hỗ trợ quyết định nhân sự chính xác hơn.

