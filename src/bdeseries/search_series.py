"""
Search for series in the BdE database

This function takes a list of search strings and returns a list of dataframes containing the results for each search string.

The list of search strings are all matched AND-wise at the list element level, and OR-wise at the word level within a list element.

Example:

search_str = ["Economía internacional", "Italia"] would be matched against the chosen field as follows: ("Economía internacional" OR "Italia") AND ("Italia")

By default, search_series() matches each field "descripcion" of the Banco de España series catalog. However, the field to be matched can be modified by passing the name of the variable to be matched against to argument 'field', i.e.: search_series(search_str=["economía internacional", "España"], field="unidades")
"""


def search_series(search_str: list[str], field: str = "description"):
    pass
