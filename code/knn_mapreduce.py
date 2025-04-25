from mrjob.job import MRJob
from collections import Counter
import pandas as pd
import numpy as np
import os

class KNNClassifier(MRJob):
    """
    A MapReduce job to classify unlabeled Iris samples using the K-Nearest Neighbors algorithm.
    """
    
    def mapper_init(self):
        """
        Initializes the mapper by loading and normalizing the Iris dataset.
        Separates labeled and unlabeled samples.
        """
        self.features = ['SepalLengthCm', 'SepalWidthCm', 'PetalLengthCm', 'PetalWidthCm']
        
        # Dynamically determine the absolute path to Iris.csv
        base_dir = os.path.dirname(__file__)
        iris_file_path = os.path.join(base_dir, "../data/Iris.csv")
        
        # Load Iris dataset
        self.iris_df = pd.read_csv(iris_file_path)
        
        # Normalize features for all rows
        self.min_values = self.iris_df[self.features].min()
        self.max_values = self.iris_df[self.features].max()
        self.iris_df[self.features] = (self.iris_df[self.features] - self.min_values) / (self.max_values - self.min_values)
        
        # Separate labeled and unlabeled rows
        self.labeled_df = self.iris_df[self.iris_df['Species'].notna()]
        self.unlabeled_df = self.iris_df[self.iris_df['Species'].isna()]

    def mapper(self, _, line):
        """
        Mapper: Calculates the Euclidean distance between unlabeled and labeled samples.
        Emits the distances along with the species of labeled samples.
        """
        
        for _, unknown_row in self.unlabeled_df.iterrows():
            unknown_id = int(unknown_row['Id'])
            unknown_features = unknown_row[self.features].values
            
            # Calculate distances to all labeled rows
            for _, labeled_row in self.labeled_df.iterrows():
                known_species = labeled_row['Species']
                known_features = labeled_row[self.features].values
                distance = np.sqrt(np.sum((unknown_features - known_features) ** 2))
                yield unknown_id, (distance, known_species)
    

    def reducer(self, unknown_id, values):
        """
        Reducer: Determines the majority species among the K nearest neighbors.
        """
        K = 15  # Number of neighbors
        neighbors = sorted(values, key=lambda x: x[0])[:K]  # Sort by distance and take K nearest
        species_count = Counter([species for _, species in neighbors])
        majority_species = max(species_count, key=species_count.get)
        yield unknown_id, majority_species


if __name__ == '__main__':
    base_dir = os.path.dirname(os.path.abspath(__file__))
    input_path = os.path.join(base_dir, "../data/Iris.csv")
    output_path = os.path.join(base_dir, "../results/knn_output.txt")

    # Run the MRJob and save results to the output file
    with open(output_path, 'w') as f:
        mr_job = KNNClassifier(args=[input_path])
        with mr_job.make_runner() as runner:
            runner.run()
            for key, value in mr_job.parse_output(runner.cat_output()):
                f.write(f"{key}: {value}\n")
                print(f"{key}: {value}")  # Print to terminal as well
