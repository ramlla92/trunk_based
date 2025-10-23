
SELECT * FROM VALUES
    (202223, '2022/23', DATE '2022-07-08', DATE '2023-07-07'),
    (202324, '2023/24', DATE '2023-07-08', DATE '2024-07-07')
AS Dim_Date (
    date_key,
    year,
    year_start_date,
    year_end_date
)