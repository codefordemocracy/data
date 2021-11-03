def remove_titles_end(name):
    if name.endswith(" MR"):
        name = name[:-3]
    elif name.endswith(" MS"):
        name = name[:-3]
    elif name.endswith(" MRS"):
        name = name[:-4]
    elif name.endswith(" HON"):
        name = name[:-4]
    elif name.endswith(" ESQ"):
        name = name[:-4]
    elif name.endswith(" REV"):
        name = name[:-4]
    elif name.endswith(" FR"):
        name = name[:-3]
    elif name.endswith(" DR"):
        name = name[:-3]
    elif name.endswith(" DR ND"):
        name = name[:-6]
    elif name.endswith(" DR DO"):
        name = name[:-6]
    elif name.endswith(" MD"):
        name = name[:-3]
    elif name.endswith(" JD"):
        name = name[:-3]
    elif name.endswith(" MBA"):
        name = name[:-4]
    elif name.endswith(" PHD"):
        name = name[:-4]
    elif name.endswith(" RET"):
        name = name[:-4]
    elif name.endswith(" (RET)"):
        name = name[:-6]
    elif name.endswith(" MSGT"):
        name = name[:-5]
    elif name.endswith(" USAF"):
        name = name[:-5]
    elif name.endswith(" USN"):
        name = name[:-4]
    elif name.endswith(" CDR"):
        name = name[:-4]
    elif name.endswith(" SGT"):
        name = name[:-4]
    elif name.endswith(" MAJ"):
        name = name[:-4]
    elif name.endswith(" THE"):
        name = name[:-4]
    return name

def remove_titles_start(name):
    if name.startswith("DR "):
        name = name[3:]
    return name

def process_name(name):
    name = name.upper()
    name = name.replace(".", "")
    if name.endswith(", LLC"):
        name = name.replace(", LLC", " LLC")
    if name.endswith(", INC"):
        name = name.replace(", INC", " INC")
    name = remove_titles_end(name)
    name = remove_titles_end(name)
    name = remove_titles_end(name)
    if "," in name:
        if name.endswith(" JR"):
            name = name[:-3]
            name = remove_titles_end(remove_titles_end(remove_titles_end(remove_titles_end(name.split(",")[1])))) + " " + remove_titles_end(remove_titles_end(remove_titles_end(remove_titles_end(name.split(",")[0])))) + " JR"
        elif name.endswith(" SR"):
            name = name[:-3]
            name = remove_titles_end(remove_titles_end(remove_titles_end(remove_titles_end(name.split(",")[1])))) + " " + remove_titles_end(remove_titles_end(remove_titles_end(remove_titles_end(name.split(",")[0])))) + " SR"
        elif name.endswith(" II"):
            name = name[:-3]
            name = remove_titles_end(remove_titles_end(remove_titles_end(remove_titles_end(name.split(",")[1])))) + " " + remove_titles_end(remove_titles_end(remove_titles_end(remove_titles_end(name.split(",")[0])))) + " II"
        elif name.endswith(" III"):
            name = name[:-4]
            name = remove_titles_end(remove_titles_end(remove_titles_end(remove_titles_end(name.split(",")[1])))) + " " + remove_titles_end(remove_titles_end(remove_titles_end(remove_titles_end(name.split(",")[0])))) + " III"
        elif name.endswith(" IV"):
            name = name[:-3]
            name = remove_titles_end(remove_titles_end(remove_titles_end(remove_titles_end(name.split(",")[1])))) + " " + remove_titles_end(remove_titles_end(remove_titles_end(remove_titles_end(name.split(",")[0])))) + " IV"
        else:
            name = remove_titles_end(remove_titles_end(remove_titles_end(remove_titles_end(name.split(",")[1])))) + " " + remove_titles_end(remove_titles_end(remove_titles_end(remove_titles_end(name.split(",")[0]))))
    name = name.replace("  ", " ")
    name = name.strip()
    name = remove_titles_start(name)
    return name
