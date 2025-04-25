## **Project Structure**

├── code                           # Python scripts for tasks 
│   ├── Frobenius_norm.py
│   ├── knn_mapreduce.py
│   ├── Reverse_web_graph.py
│   ├── top_keywords_per_genre.py
├── data                           # Inputs directory 
│   ├── A.txt
│   ├── Iris.csv
│   ├── movies.csv
│   ├── web-Google.txt
├── results                        # Outputs directory
│   ├── frobenius_output.txt
│   ├── knn_output.txt
│   ├── reverse_web_graph_output.txt
│   ├── top_keywords_by_genre.txt
├── README.md                      # Project description and instructions
├── requirements.txt               # Required Python dependencies

## **Python Version**
   - This project requires Python **3.11.5**.

## **Dependencies**
- `mrjob`: Framework for MapReduce in Python.
- `numpy`: Numerical computations (used in KNN).
- `pandas`: Dataset handling and preprocessing.
- `nltk`: Text processing for stopwords filtering.

Install all dependencies using:
```bash
pip install -r requirements.txt
```

## **How to Run the Project**

1. **Reverse Web Graph**:
    ```bash
    python code/Reverse_web_graph.py
    ```
2. **K-Nearest Neighbors Classification**:
    ```bash
    python code/knn_mapreduce.py
    ```
3. **Top Keywords by Genre**:
    ```bash
    python code/top_keywords_per_genre.py
    ```
4. **Frobenius Norm**:
    ```bash
    python code/Frobenius_norm.py
    ```