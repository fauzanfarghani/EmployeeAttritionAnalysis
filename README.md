#  Analisis Tingkat Attrition Karyawan

## Repository Outline
Berikut adalah isi dari file yang ada di repository ini:

```
  P2-M3/fauzanfarghani
  |
  ├── description.md = Penjelasan gambaran umum proyek
  ├── P2M3_fauzanfarghani_ddl.txt = Query yang digunakan untuk loading dataset ke postgre
  ├── P2M3_fauzanfarghani_data_raw.csv = data kotor yang diperoleh dari website Kaggle
  ├── P2M3_fauzanfarghani_data_clean.csv = data yang sudah dibersihkan menggunakan automasi Airflow
  ├── P2M3_fauzanfarghani_DAG.py = program automasi untuk data cleaning menggunakan Airflow
  ├── P2M3_fauzanfarghani_DAG_graph.jpg = Alur grafik automasi
  ├── P2M3_fauzanfarghani_conceptual.txt = informasi umum terkait NoSQL, Airflow, Great Expectation dan Batch Processing.
  ├── P2M3_fauzanfarghani_GX.ipynb = File notebook untuk validasi data dari dataset bersih menggunakan great expectation.
  ├── /images = visualisasi dari analisis data beserta penjelasannya berisikan foto-foto berikut
        ├── introduction & objective.png
        ├── plot & insight 01.png
        ├── plot & insight 02.png
        ├── plot 03.png
        ├── insight 03.png
        ├── plot & insight 04.png
        ├── plot & insight 05.png
        ├── plot & insight 06.png
        ├── kesimpulan.png
        └── rekomendasi bisnis.png
```

## Problem Background
Perpindahan atau pengunduran diri karyawan merupakan salah satu tantangan utama dalam manajemen sumber daya manusia di berbagai perusahaan. Dataset menyediakan data tentang faktor-faktor yang memengaruhi keputusan karyawan untuk tetap bekerja atau meninggalkan perusahaan. Dataset ini mencakup variabel seperti usia, tingkat pendidikan, kepuasan kerja, jarak dari rumah, gaji bulanan, dan lainnya. Tingginya tingkat attrition dapat menyebabkan kerugian finansial, penurunan produktivitas, dan hilangnya talenta. Oleh karena itu, analisis data ini diperlukan untuk mengidentifikasi pola dan faktor utama yang berkontribusi terhadap attrition, sehingga perusahaan dapat merancang strategi retensi yang efektif.

## Project Output
Output dari hasil proyek ini adalah sebagai berikut:
- Visualisasi Exploratory Data Analysis terkait dataset yang telah dibersihkan menggunakan automasi Airflow
- Kesimpulan dari analisis yang telah dilakukan
- Rekomendasi Bisnis yang dapat diterapkan berdasarkan kesimpulan yang telah dibuat.

## Data
Data yang digunakan adalah dataset `P2M3_fauzanfarghani_data_clean.csv.csv` yang berisi informasi profil dari karyawan secara anonim dengan kolom-kolom berikut:
- **Jumlah kolom**: 35
- **Jumlah baris**: 1470 baris.
- **Sumber data**: https://www.kaggle.com/datasets/pavansubhasht/ibm-hr-analytics-attrition-dataset

## Method
Metode yang digunakan adalah menginput data menggunakan automasi airflow, dalam automasi tersebut sudah melibatkan proses pengambilan data dari postgre, pembersihan data, dan input data ke Kibana untuk kebutuhan visualisasi yang nantinya akan menghasilkan sebuah dashboard visualisasi analisis data.

## Stacks
- **Bahasa Pemrograman**: Python, SQL, NoSQL.
- **Tools**: Jupyter Notebook, VSCode.
- **Library Python**:
  - `Airflow` = Automasi pembersihan data
  - `ElasticSearch` = Input dataset yang sudah dibersihkan ke Kibana dan melakukan visualisasi
  - `great_expectations` = Validasi dataset bersih
  - `Pandas` = Akses Dataframe

## Reference
- ElasticSearch Documentation: https://www.elastic.co/guide/index.html
- Kibana Documentation: https://www.elastic.co/guide/en/kibana/7.13/index.html
- Airflow Documentation: https://airflow.apache.org/docs/
- Great Expectation Documentation: 
- Dataset: https://www.kaggle.com/datasets/pavansubhasht/ibm-hr-analytics-attrition-dataset
