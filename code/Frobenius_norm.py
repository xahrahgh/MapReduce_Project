from mrjob.job import MRJob
from mrjob.step import MRStep
import os

class FrobeniusNorm(MRJob):
    """
    A MapReduce job to calculate the Frobenius norm of a matrix.
    """

    def steps(self):
        """
        Defines the mapper, combiner, and reducer steps for the MapReduce job.
        """

        return [
            MRStep(mapper=self.mapper_square_elements,
                   combiner=self.combiner_row_sum,
                   reducer=self.reducer_calculate_norm)
        ]

    def mapper_square_elements(self, _, line):
        """
        Mapper: Emits the square of each matrix element.
        """

        row = [float(x) for x in line.split()]
        for value in row:
            yield None, value ** 2
    
    def combiner_row_sum(self, key, values):
      """
        Combiner: Computes the sum of squared values for each row.
        """
      
      yield key, sum(values)

    def reducer_calculate_norm(self, key, values):
        """
        Reducer: Calculates the Frobenius norm from the row sums.
        """

        total_sum = sum(values)
        frobenius_norm = total_sum ** 0.5
        yield "Frobenius_Norm", frobenius_norm

if __name__ == '__main__':
    # Define input and output file paths
    base_dir = os.path.dirname(os.path.abspath(__file__))
    input_path = os.path.join(base_dir, "../data/A.txt")
    output_path = os.path.join(base_dir, "../results/frobenius_output.txt")


    # Run the MRJob and save results to the output file
    with open(output_path, 'w') as f:
        mr_job = FrobeniusNorm(args=[input_path])
        with mr_job.make_runner() as runner:
            runner.run()
            for key, value in mr_job.parse_output(runner.cat_output()):
                f.write(f"{key}: {value}\n")
                print(f"{key}: {value}")  # Print to terminal as well
