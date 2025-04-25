import os
from mrjob.job import MRJob
from mrjob.step import MRStep

class ReverseWebGraph(MRJob):
    """
    A MapReduce job to reverse edges in a web-link graph.
    """
    def steps(self):
        return [
            MRStep(mapper=self.mapper_reverse_links,
                   reducer=self.reducer_group_sources)
        ]

    def mapper_reverse_links(self, _, line):
        """
        Mapper: Reads each line, filters out comments, and outputs <ToNodeID, FromNodeID>.
        """
        # Skip comment lines
        if line.startswith("#"):
            return

        # Split the line into from_node and to_node
        parts = line.strip().split()
        if len(parts) != 2:
            return

        from_node, to_node = parts[0], parts[1]

        # Emit <to_node, from_node>
        yield to_node, from_node

    def reducer_group_sources(self, to_node, from_nodes):
        """
        Reducer: Groups all from_node for a given to_node.
        """
        yield to_node, list(from_nodes)


if __name__ == '__main__':
    base_dir = os.path.dirname(os.path.abspath(__file__))
    input_path = os.path.join(base_dir, "../data/web-Google.txt")
    output_path = os.path.join(base_dir, "../results/reverse_web_graph_output.txt")


    # Run the MRJob and save results to the output file
    with open(output_path, 'w') as f:
        mr_job = ReverseWebGraph(args=[input_path])
        with mr_job.make_runner() as runner:
            runner.run()
            for key, value in mr_job.parse_output(runner.cat_output()):
                f.write(f"{key}: {value}\n")
