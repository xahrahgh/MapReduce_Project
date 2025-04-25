from mrjob.job import MRJob
from mrjob.step import MRStep
from collections import Counter
import re
import csv
from nltk.corpus import stopwords
import nltk
import os

# Ensure NLTK stopwords are available
nltk.download('stopwords', quiet=True)

class TopKeywordsByGenre(MRJob):
    """
    A MapReduce job to identify the top 10 keywords for each movie genre.
    """

    def steps(self):
        """
        Defines the mapper and reducer steps for the MapReduce job.
        """

        return [
            MRStep(mapper=self.mapper_extract_keywords,
                   reducer=self.reducer_count_keywords),
            MRStep(reducer=self.reducer_top_keywords)
        ]

    def mapper_extract_keywords(self, _, line):
        """
        Mapper: Extracts keywords from movie titles and associates them with genres.
        """

        # skips header line
        if line.startswith("movieId"):
            return
        
        # gets rid of the the malformed lines, then devide to title and genre
        for row in csv.reader([line]):
            if len(row) < 3:
                return
            title = row[1]
            genres = row[2]

        # extract words from titles
        words = re.findall(r'\b[a-z]+\b', title.lower())

        # load stopwords
        stop_words = set(stopwords.words('english')).union(stopwords.words('french'))
        custom_stop_words = {'vs', 'aka', 'a.k.a.', 'et', 'al', 'i', 'ii', 'iii', 'iv', 'v', 'vi', 'vii', 'viii', 'ix', 'x'}
        stop_words.update(custom_stop_words)

        # filter keywords
        keywords = [word for word in words if word not in stop_words and len(word) > 1]

        # emit each keyword for each genre
        for genre in genres.split('|'):
            for word in keywords:
                yield (genre, word), 1

    def reducer_count_keywords(self, key, values):
        """
        Reducer: Counts occurrences of each keyword for a genre.
        """

        genre, word = key
        yield genre, (word, sum(values))

    def reducer_top_keywords(self, genre, word_counts):
        """
        Reducer: Identifies the top 10 keywords for each genre.
        """

        counts = Counter()
        for word, count in word_counts:
            counts[word] += count
        yield genre, counts.most_common(10)

if __name__ == '__main__':
    base_dir = os.path.dirname(os.path.abspath(__file__))
    input_path = os.path.join(base_dir, "../data/movies.csv")
    output_path = os.path.join(base_dir, "../results/top_keywords_by_genre.txt")


    with open(output_path, 'w') as f:
        mr_job = TopKeywordsByGenre(args=[input_path])
        with mr_job.make_runner() as runner:
            runner.run()
            for key, value in mr_job.parse_output(runner.cat_output()):
                f.write(f"{key}: {value}\n")
                
    print("Job complete! Results have been written to top_keywords_by_genre.txt.")