import numpy as np
import psycopg2
import matplotlib.pyplot as plt

db_params = {
    "host": "localhost",
    "database": "NobelPrizeWinners",
    "user": "Shkarupylo",
    "password": "Shkarupylo",
    "port": "5432",
}


query_1 = '''
CREATE VIEW city_laureate_part AS
WITH laur_prizes as
(
    SELECT 
        pr.category, o.organizationcountry 
    FROM
        laureate as l 
        INNER JOIN organization as o ON l.organizationname = o.organizationname
        INNER JOIN prizelaureates as lp ON l.laureateid = lp.laureateid
        INNER JOIN prize as pr ON lp.id = pr.id
)
SELECT organizationcountry, category, COUNT(category) as number_prizes_in_categories FROM laur_prizes
GROUP BY organizationcountry, category
ORDER BY organizationcountry;
'''
query_2 = '''
CREATE VIEW women_part AS
WITH laur_prizes as
(
    SELECT 
        pr.category, l.gender 
    FROM
        laureate as l 
        INNER JOIN prizelaureates as lp ON l.laureateid = lp.laureateid
        INNER JOIN prize as pr ON lp.id = pr.id
)
SELECT category, COUNT(category) as number_prizes_in_categories FROM laur_prizes
WHERE gender = 'female'
GROUP BY gender, category
ORDER BY gender;
'''
query_3 = '''
CREATE VIEW city_men_part AS

WITH laur_with_city as
(
    SELECT 
        l.gender, o.organizationcity
    FROM
        laureate as l INNER JOIN organization as o ON l.organizationname = o.organizationname
)
SELECT organizationcity, COUNT(gender) as number_males FROM laur_with_city
WHERE gender = 'male'
GROUP BY organizationcity, gender
ORDER BY number_males DESC
'''

conn = psycopg2.connect(**db_params)

with conn:
    cur = conn.cursor()
    cur.execute('DROP VIEW IF EXISTS city_laureate_part')
    cur.execute(query_1)
    cur.execute('SELECT * FROM city_laureate_part LIMIT 20;')
    country = []
    category_number = []

    matplotlib_colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k']

    for row in cur:
        if country == [] or country[-1] != row[0]:
            country.append(row[0])
            category_number.append([row[1:]])
        else:
            category_number[-1].append(row[1:])

    categories = np.array([item for sublist in category_number for item in sublist]).T[0]
    categories = set(categories)
    categories_colors = {}
    for j, i in enumerate(categories):
        categories_colors[i] = matplotlib_colors[j]

    figure, (bar_ax, pie_ax, graph_ax) = plt.subplots(1, 3, figsize=(20, 10))

    x = np.arange(len(country))  # the label locations
    width = 0.25  # the width of the bars
    multiplier = 0

    for i, cn in enumerate(category_number):
        category, number = np.array(cn).T
        number = list(map(int, number))
        xs = [x[i] + width * j for j in range(len(category))]
        bar_ax.bar(xs, number, width, label=category, color=[categories_colors[c] for c in category])

    # Add some text for labels, title and custom x-axis tick labels, etc.
    bar_ax.set_ylabel('Number')
    bar_ax.set_title('Кількість лауреатів з кожної країни\n(по категоріям)')
    bar_ax.set_xticks(x + width, country, rotation=45)
    # bar_ax.legend(loc='upper left', ncols=3)
    bar_ax.set_ylim(0, 5)

    cur.execute('DROP VIEW IF EXISTS women_part')
    cur.execute(query_2)
    cur.execute('SELECT * FROM women_part;')
    category = []
    total = []

    for row in cur:
        category.append(row[0])
        total.append(row[1])

    x_range = range(len(category))
    pie_ax.pie(total, labels=category, autopct='%1.1f%%')
    pie_ax.set_title('Частка жінок лауреатів\nу кожній категорії')

    # cur.execute(query_3)
    cur.execute('DROP VIEW IF EXISTS city_men_part')
    cur.execute(query_3)
    cur.execute('SELECT * FROM city_men_part LIMIT 10;')
    city = []
    males_num = []

    for row in cur:
        city.append(row[0])
        males_num.append(row[1])

    mark_color = 'blue'
    graph_ax.plot(city, males_num, color=mark_color, marker='o')

    for qnt, price in zip(city, males_num):
        graph_ax.annotate(price, xy=(qnt, price), color=mark_color,
                          xytext=(7, 2), textcoords='offset points')

    graph_ax.set_xlabel('Назва міста')
    graph_ax.set_ylabel('Кількість лауреатів')
    graph_ax.set_xticklabels(city, rotation=45, ha="right")
    graph_ax.plot(city, males_num, color='blue', marker='o')
    graph_ax.set_title('Міста за кількістю\nчоловіків лауреатів')

mng = plt.get_current_fig_manager()
mng.full_screen_toggle()
plt.savefig('new_graphs.png', bbox_inches='tight')
plt.show()