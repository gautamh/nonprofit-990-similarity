import csv

INPUT_FEATURES_FILE = "/mnt/c/Users/Gautam/nonprofit_features.csv"

OUTPUT_FEAUTRES_FILE = "/mnt/c/Users/Gautam/nonprofit_features_ntee.csv"

CODE_FILES = [
    "/mnt/c/Users/Gautam/Downloads/eo1.csv",
    "/mnt/c/Users/Gautam/Downloads/eo2.csv",
    "/mnt/c/Users/Gautam/Downloads/eo3.csv",
    "/mnt/c/Users/Gautam/Downloads/eo4.csv"
]

ntee_codes_map = {}
output_features = []
header = []

for file in CODE_FILES:
    with open(file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            ntee_codes_map[row["EIN"]] = row["NTEE_CD"]

with open(INPUT_FEATURES_FILE, 'r') as g:
    reader = csv.reader(g);
    header = next(reader)
    for row in reader:
        ntee_code = ntee_codes_map.get(row[0], "Z00")
        if ntee_code is None or ntee_code.strip() == "":
            ntee_code = "Z00"
        elif len(ntee_code) > 3:
            ntee_code = ntee_code[:3]
        row.append(ntee_code)
        output_features.append(row)

with open(OUTPUT_FEAUTRES_FILE, 'w') as h:
    writer = csv.writer(h)
    header.append("NTEE_CD")
    writer.writerow(header)
    writer.writerows(output_features)

