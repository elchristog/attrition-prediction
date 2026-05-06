import json
import os

notebook_path = r'c:\Users\W515059\Documents\nam_attrition\attrition_pipeline\python\pipeline_testing.ipynb'

with open(notebook_path, 'r', encoding='utf-8') as f:
    nb = json.load(f)

for cell in nb['cells']:
    if cell['cell_type'] == 'code':
        source_str = "".join(cell['source'])
        
        # Update MultiHorizonProfiler targets
        if "MultiHorizonProfiler(targets=['TARGET_6M', 'TARGET_8M', 'TARGET_12M'])" in source_str:
            print("Updating MultiHorizonProfiler targets...")
            new_source = source_str.replace(
                "MultiHorizonProfiler(targets=['TARGET_6M', 'TARGET_8M', 'TARGET_12M'])",
                "MultiHorizonProfiler(targets=['TARGET_3M', 'TARGET_6M', 'TARGET_8M', 'TARGET_12M'])"
            )
            # Update the cell source
            cell['source'] = [line + '\n' for line in new_source.split('\n')]
            if cell['source'][-1] == '\n': cell['source'].pop()
            else: cell['source'][-1] = cell['source'][-1].rstrip('\n')

        # Also sort by TARGET_3M in the comparison table pivot if it was sorting by 6M
        if "display(pivot_iv.sort_values('TARGET_6M', ascending=False).head(10))" in source_str:
            print("Updating pivot sorting...")
            new_source = source_str.replace(
                "display(pivot_iv.sort_values('TARGET_6M', ascending=False).head(10))",
                "display(pivot_iv.sort_values('TARGET_3M', ascending=False).head(10))"
            )
            cell['source'] = [line + '\n' for line in new_source.split('\n')]
            if cell['source'][-1] == '\n': cell['source'].pop()
            else: cell['source'][-1] = cell['source'][-1].rstrip('\n')

with open(notebook_path, 'w', encoding='utf-8') as f:
    json.dump(nb, f, indent=1)

print("Notebook updated successfully.")
