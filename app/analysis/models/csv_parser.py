import csv

from app.util import weight_calculator

if __name__ == '__main__':
    input = open('source.csv', 'r', encoding='utf-8')
    reader = csv.reader(input)
    output = open('output.csv', 'w', encoding='utf-8', newline='')
    writer = csv.writer(output)
    # for row in reader:
    #     spec = row[6]
    #     product_type = row[7]
    #     quantity = row[11]
    #     if quantity == '':
    #         quantity = 0
    #     free_pcs = row[12]
    #     if free_pcs == '':
    #         free_pcs = 0
    #     row[3] = weight_calculator.calculate_weight(product_type, spec, quantity, free_pcs)
    #     print(row[3])
    #     writer.writerow(row)
    for row in reader:
        weight = int(row[3])
        if weight > 0 and weight < 34000:
            writer.writerow(row)
