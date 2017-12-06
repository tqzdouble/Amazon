import csv
with open('query_by_profile.csv', newline='') as csvfile:
     spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
spamreader

from wordcloud import WordCloud


#read first column of csv file to string of words seperated


your_list = []
with open('query_by_profile.csv', 'rb') as f:
    reader = csv.reader(f)
    your_list = '\t'.join([i[9] for i in reader])


# Generate a word cloud image
wordcloud = WordCloud().generate(your_list)

# Display the generated image:
# the matplotlib way:
import matplotlib.pyplot as plt
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis("off")

# lower max_font_size
wordcloud = WordCloud(max_font_size=).generate(your_list)
plt.figure()
plt.imshow(wordcloud, interpolation="bilinear")
plt.axis("off")
plt.show()
