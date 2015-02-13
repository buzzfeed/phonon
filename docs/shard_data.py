from matplotlib import pyplot


fig, ax = pyplot.subplots()

n, bins, patches = pyplot.hist([1.], label=['Reads', 'Writes'], bins=2) 

labels = [(0.75, 'Reads'), (1.25, 'Writes')]
for x, label in labels:
    ax.annotate(label, xy=(x, 0), xycoords=('data', 'axes fraction'),
            xytext=(0, -18), textcoords='offset points', va='top', ha='center')

ax.set_xticks(bins)
ax.get_xaxis().set_visible(False)
ax.set_title('Percent of Queries to Shard Served')
pyplot.show()
