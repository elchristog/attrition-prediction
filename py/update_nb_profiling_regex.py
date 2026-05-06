import json
import re

notebook_path = r'c:\Users\W515059\Documents\nam_attrition\attrition_pipeline\python\pipeline_testing.ipynb'

with open(notebook_path, 'r', encoding='utf-8') as f:
    nb = json.load(f)

for cell in nb['cells']:
    if cell['cell_type'] == 'code':
        new_source = []
        changed = False
        for line in cell['source']:
            # Search for MultiHorizonProfiler initialization
            if 'MultiHorizonProfiler(targets=[' in line:
                if 'TARGET_3M' not in line:
                    print(f"Found targets line: {line.strip()}")
                    # Replace whatever is in targets=[...] with the 4-horizon list
                    line = re.sub(r"targets=\[.*?\]", "targets=['TARGET_3M', 'TARGET_6M', 'TARGET_8M', 'TARGET_12M']", line)
                    changed = True
            
            # Search for the pivot sort
            if "pivot_iv.sort_values(" in line:
                if 'TARGET_3M' not in line:
                    print(f"Found sort line: {line.strip()}")
                    line = re.sub(r"sort_values\(.*?,", "sort_values('TARGET_3M',", line)
                    changed = True
            
            new_source.append(line)
        
        if changed:
            cell['source'] = new_source

with open(notebook_path, 'w', encoding='utf-8') as f:
    json.dump(nb, f, indent=1)

print("Notebook updated successfully with regex.")
