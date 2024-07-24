import csv

# Define the input and output file names
input_file = 'C:\\Users\\renzo\\Desktop\\hpc\\csv\\ROkzNjh5SCO63dSgeX8tcw.csv'
output_file = 'C:\\Users\\renzo\\Desktop\\hpc\\csv\\ROkzNjh5SCO63dSgeX8tcw2.csv'

# Number of lines to keep
max_lines = 1000000

# Open the input file and the output file
with open(input_file, 'r', newline='') as infile, open(output_file, 'w', newline='') as outfile:
    reader = csv.reader(infile)
    writer = csv.writer(outfile)
    
    # Write the header
    header = next(reader)
    writer.writerow(header)
    
    # Write the first max_lines rows
    for i, row in enumerate(reader):
        if i >= max_lines:
            break
        writer.writerow(row)

print(f"Archivo recortado a {max_lines} líneas y guardado en {output_file}")
