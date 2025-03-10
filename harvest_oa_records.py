import openalex as oa

#print(openalex.query(openalex.BASE_URL, openalex.UWINNIPEG_ID, 1))
#print(openalex.total_results(openalex.query(openalex.BASE_URL, openalex.UWINNIPEG_ID, 1)))
#print(oa.parse_results(oa.query(oa.BASE_URL, oa.UWINNIPEG_ID, 1)))
oa.write_dspace_data(oa.parse_results(oa.query(oa.BASE_URL, oa.UWINNIPEG_ID, 1)))
