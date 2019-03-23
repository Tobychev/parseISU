import re
import os
import camelot
import warnings
import pandas as pd
from subprocess import check_output

mods = re.compile("\W")
digits = re.compile("\d")
box = pd.read_csv("box_edges.cvs",comment="#").set_index(["type","class","segment","num","part"])

def parse_element(element_string,info):
    element_string = element_string.split("+")
    elements = []
    infos = []
    for element in element_string:
        el = {}
        if element[0].isdigit(): # A jump
            el["rot"] = int(element[0])
            el["type"] = mods.sub("",element[1:])
            mod = mods.search(element)
            if mod:
                infos.append(element[mod.start():mod.end()+1])
                el["mods"] = [infos[-1]]

            if el["type"] in ["Lz","F"] and "!" in info:
                if "mods" in el.keys():
                    el["mods"] = el["mods"] + ["!"]
                else:
                    el["mods"] = ["!"]
            elements.append(el)
        elif element == "COMBO": # A failed jump combination
            el["type"] = "COMBO"
            elements.append(el)
        elif element[:2] in ["CC","FC","LS","St","SS"]: # A spin
            # Handle error in the spin first
            if "V" in element:
                infos.append("V")
                el["mods"] = infos[-1]
                element = re.sub("V","",element)
            # Find type
            el["type"] = digits.sub("",element)
            
            # Find level
            digit = digits.search(element)
            if digit:
                el["lev"] = element[digit.start():digit.end()+1]
            elements.append(el)
        else: # A simple jump
            el["type"] = mods.sub("",element)
            mod = mods.search(element)
            if mod:
                infos.append(element[mod.start():mod.end()+1])
                el["mods"] = [infos[-1]]

            if el["type"] in ["Lz","F"] and "!" in info:
                el["mods"] = el["mods"] + ["!"]
            elements.append(el)
            
    return elements,infos

def parse_header(table):
    result = {}
    offset = 0
    result["skater"]  = table.df[1][3]
    result["start_no"] = int(table.df[3][3])
    result["segment_rank"] = int(table.df[0][3])
    result["TSS"] = float(table.df[4][3])
    result["TES"] = float(table.df[5][3])
    if "B" in table.df[6][3]:
        offset = 1
    result["PCS"] = float(table.df[6+offset][3])
    result["TDD"] = float(table.df[7+offset][3])
    return result

def parse_element_scores(table):
    results = []
    totals = {}

    for idx,row in table.df.iterrows():
        result = {}        
        if row[1] == "":
            totals["total_base"] = float(row[4])
            totals["total_score"] = float(row[17])
            continue
            
        if idx == 0:
            row[3] = row[3].replace("Info","")
        result["element"],result["info"] = parse_element(row[2],row[3])
        result["el_idx"] = int(row[1])
        result["base"] = float(row[4])
        result["bonus"] = len(row[5]) > 0
        result["goe"] = float(row[6])
        try:
            result["judges"] = [int(el) for el in row[7:16] if len(el) > 0]
        except ValueError:
            result["judges"] = []
        result["score"] = float(row[17])
        results.append(result)
        
    return totals,results

def parse_program_scores(table):
    score = {}
    score["SKS"] = {"factor": float(table.df.iloc[1,1]), 
                    "scores": [float(score.replace(",",".")) for score in table.df.iloc[1,2:-1] ],
                    "tot": float(table.df.iloc[1,-1])}
    score["TRS"] = {"factor": float(table.df.iloc[2,1]), 
                    "scores": [float(score.replace(",",".")) for score in table.df.iloc[2,2:-1] ],
                    "tot": float(table.df.iloc[2,-1])}
    score["PRF"] = {"factor": float(table.df.iloc[3,1]), 
                    "scores": [float(score.replace(",",".")) for score in table.df.iloc[3,2:-1] ],
                    "tot": float(table.df.iloc[3,-1])}
    score["CMP"] = {"factor": float(table.df.iloc[4,1]), 
                    "scores": [float(score.replace(",",".")) for score in table.df.iloc[4,2:-1] ],
                    "tot": float(table.df.iloc[4,-1])}
    score["IOM"] = {"factor": float(table.df.iloc[5,1]), 
                    "scores": [float(score.replace(",",".")) for score in table.df.iloc[5,2:-1] ],
                    "tot": float(table.df.iloc[5,-1])}
    return score

def parse_deductions(table):
    deduct = {}

    if "(" in "".join(*table.df.values):
        with_paren = True
    else:
        with_paren = False

    if len(table.df.iloc[0,1:]) > 1:
        dat_itr = iter(table.df.iloc[0,1:-1])
        for entry in dat_itr:
            deduct[ entry.replace(":","") ] = float(next(dat_itr))
            # Contains useless (1) style annotation, step over it
            if with_paren:
                next(dat_itr)

    deduct["total"] = float(table.df.iloc[0,-1])
    return deduct

def get_num_pages(pdf_path):
    output = check_output(["pdfinfo", pdf_path]).decode()
    pages_line = [line for line in output.splitlines() if "Pages:" in line][0]
    num_pages = int(pages_line.split(":")[1])
    return num_pages

def try_get_canary(file,file_type,age_class,competition_segment,page):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        canary = "{0},{2},{1},{3}".format( 
            *box.loc["isucalc",age_class,competition_segment,"2","canary"].values)

        try: 
            data = camelot.read_pdf(filepath=file, flavor="stream",
                                     table_areas=[canary], pages=page)
            return True
        except ValueError:
            return False

def get_result_from_table(file,pages,header_box,element_box,program_box,deduction_box,element_columns):
    data = camelot.read_pdf(filepath=file,
                             flavor="stream",
                             table_areas=[header_box,
                                          element_box,
                                          program_box,
                                          deduction_box],
                             columns=["",element_columns,"",""],
                             pages=pages)
    
    results = []
    
    data_itr = iter(data)
    for tab in data_itr:
        try:
            entry = {}
            entry = parse_header(tab)
            totals,elements = parse_element_scores(next(data_itr))
            pcs = parse_program_scores(next(data_itr))
            ded = parse_deductions(next(data_itr))
            entry["TES"] = elements
            entry["PCS"] = pcs
            entry["DED"] = ded
            entry["total_base"] = totals["total_base"]
            results.append(entry)
        except ValueError:
            warnings.warn("Skippar tabell på sida {} i fil {}".format(
                tab.page,file))
            next(data_itr); next(data_itr);
        except KeyError:
            warnings.warn("Skippar tabell på sida {} i fil {}".format(
                tab.page,file))
            next(data_itr); next(data_itr); next(data_itr)
        except IndexError:
            warnings.warn("Skippar tabell på sida {} i fil {}".format(
                tab.page,file))
            entry["TES"],   entry["PCS"],   entry["DED"] = None, None, None
            next(data_itr); next(data_itr);
            entry["total_base"] = None
            results.append(entry)

    return results
       

def get_result_from_page_by_type(file,file_type,age_class,competition_segment,num_on_page,pages):

    header = "{0},{2},{1},{3}".format( 
        *box.loc["isucalc",age_class,competition_segment,num_on_page,"header"].values)
    elements = "{0},{2},{1},{3}".format( 
        *box.loc["isucalc",age_class,competition_segment,num_on_page,"elements"].values)
    program  = "{0},{2},{1},{3}".format( 
        *box.loc["isucalc",age_class,competition_segment,num_on_page,"program"].values)
    deductions = "{0},{2},{1},{3}".format( 
        *box.loc["isucalc",age_class,competition_segment,num_on_page,"deductions"].values)
    
    element_columns="0,57,157,168,195,203,246,278,302,323,345,370,393,414,438,478,511,553"

    if "2" in num_on_page:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            # Last page might only have one table, so treat this sepatately
            pages,_,last = pages.rpartition(",")
            try:
                last  = get_result_from_table(file,last,header,elements,program,deductions,element_columns)
            except ValueError:
                last = []
    else:
        last = []

    if len(pages) > 0:
        results = get_result_from_table(file,pages,header,elements,program,deductions,element_columns)
    else:
        results = []

    return results+last    

def get_results(file,gender_class=None):
    num_pages = get_num_pages(file)

    # Assume for now
    file_type = "isucalc"
   
    if gender_class is None:
        if "herrar" in file.lower() or "pojkar" in file.lower():
            gender_class = 0
        else:
            gender_class = 1

    if "seniorer" in file.lower():
        age_class = "senior"
#    elif "ungdom" in file.lower():
#        age_class = "ungdom"
    else: 
        warnings.warn("Hoppar över fil {}: enbart seniorklassen stöds just nu".format(file),stacklevel=1)
        return []

    if "sp" in file.lower():
        competition_segment = "sp"
    elif "fs" in file.lower():
        competition_segment = "fs"
    else: 
        raise NotImplementedError("Enbart kort och friåkning stöds just nu")
    when = re.compile("(\d+-\d\d-\d\d)")
    date = when.search(file).group(0)

    pages = "".join([str(i)+"," for i in range(1,num_pages)])+str(num_pages)
    data = get_result_from_page_by_type(file,file_type,age_class,competition_segment,"1",pages)
    if age_class == "senior":
        data = data + get_result_from_page_by_type(file,file_type,age_class,competition_segment,"2",pages)
    else:
        twoa = []
        twob = []
        for page in range(1,num_pages+1):
            if try_get_canary(file,file_type,age_class,competition_segment,str(page)):
                twob.append(str(page))
            else:
                twoa.append(str(page))

        twoa_pages = ",".join(twoa)
        twob_pages = ",".join(twob)
        if len(twoa_pages) > 0:
            data = data + get_result_from_page_by_type(file,file_type,age_class,competition_segment,"2a",twoa_pages)
        if len(twob_pages) > 0:
            data = data + get_result_from_page_by_type(file,file_type,age_class,competition_segment,"2b",twob_pages)

    for entry in data:
        entry["competition"] = {"segment":competition_segment,
                                "gender_class":gender_class,
                                "date":date}
        
    return data
