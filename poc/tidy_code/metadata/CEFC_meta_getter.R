## R script to get metadata from ORFEO-CEFC subcorpora
# designate folder over which to work
current_folder = '/Users/Data/ANR_PREFAB/Corpus_ortolang/cefc-orfeo/11/oral/tcof//'
# get list of files, make df to hold data
target_files = list.files(current_folder, pattern= ".xml", full.names = T)
holding_data <- data.frame()
# current_file = target_files[1]

# parsed_data <- read_html(current_file)


# iterate over files
for (current_file in target_files){
 # get the HTML parse 
 parsed_data <- read_html(current_file)
 ## extract title and trim
 file_name <- parsed_data %>% 
  html_elements(xpath = "//title") %>% 
  # html_text() %>% 
  html_text(trim = T)
 ## extract responsable_name
 resps <- parsed_data %>% 
  html_elements(xpath = "//respstmt/name") %>% 
  html_text(trim = T) %>% 
  paste(collapse = ", ")

 ## extract place, set region and city to blank  
 blank_place_tidy <- ""
 place_country <- parsed_data %>% 
  html_elements(xpath = "//settingdesc/place") %>% 
  html_text(trim = T)
  
 place_region <- ""
 place_city <- ""
 ## get abstract if present, tidy 
 resume <- parsed_data %>% 
  html_elements(xpath = "//profiledesc/abstract") %>% 
  html_text(trim=T)
 resume <- gsub("\n", " ", resume)
 resume <- gsub("( )+", " ", resume)
 ## get date and tryto parse as dmy format ; on error, usually only a year present, so get last 4 chars 
 year_enreg <- parsed_data %>% 
  html_elements(xpath = "//recordingstmt//date") %>% 
  html_text() %>% 
  lubridate::dmy() %>% 
  year()
 if (is.na(year_enreg) == TRUE){
  year_enreg = parsed_data %>% 
   html_elements(xpath = "//recordingstmt//date") %>% 
   html_text(trim=T) 
  year_enreg <- substr(year_enreg, str_length(year_enreg)-3, str_length(year_enreg) )
 }
 ## get recording duration in seconds 
 duree_enreg <- parsed_data %>% 
  html_elements(xpath = "//recordingstmt//recording/@dur") %>% 
  lubridate::hms() %>% 
  lubridate::period_to_seconds()
 ## get recording sound quality and tidy 
 qualite_son <- parsed_data %>% 
  html_elements(xpath = "//recordingstmt//recording/@ana") %>% 
  html_text() 
 qualite_son <- gsub(" #audio/wav #NULL","", qualite_son)
 qualite_son <- gsub("^#|_","", qualite_son)
 ## add anonymisation value as UNK 
 anon_signal <- "UNK"
 ## add interaction type and set to UNK on error
 type_interaction <- parsed_data %>% 
  html_elements(xpath = "//textclass//catref[contains(@corresp, 'type')]") %>% 
  html_element(xpath = "@target") %>% 
  html_text()
 if (length(type_interaction) ==0){type_interaction <- "UNK"}
 ## add sector and set to UNK on error 
 secteur <- parsed_data %>% 
  html_elements(xpath = "//textclass//catref[contains(@corresp, 'secteur')]") %>% 
  html_element(xpath = "@target") %>% 
  html_text()
 if (length(secteur) ==0){secteur <- "UNK"}
 
 ## add milieu and set to UNK on error
 milieu <- parsed_data %>% 
  html_elements(xpath = "//textclass//catref[contains(@corresp, 'milieu')]") %>% 
  html_element(xpath = "@target") %>% 
  html_text()
 if (length(milieu) ==0){milieu <- "UNK"}

 ## add situation type and set to UNK on error 
 situation <- parsed_data %>% 
  html_elements(xpath = "//textclass//catref[contains(@corresp, 'channel')]") %>% 
  html_element(xpath = "@target") %>% 
  html_text()
 if (length(situation) ==0){situation <- "UNK"}

 ## add number of speakers and if greater than 2, set to 2+ as str
 num_loc <- parsed_data %>% 
  html_elements(xpath = "//textclass//catref[contains(@corresp, 'nbLocuteurs')]") %>% 
  html_element(xpath = "@target") %>% 
  html_text()
 if(num_loc >2){ num_loc <- "2+"}
 ## get list of speakers from xml attribute and collapse to comma separated string 
 speakers <- xml_attr(xml_find_all(parsed_data, "//person"), "xml:id") %>% 
  paste(collapse = ", ")
 
 ## get list of speaker ages and collapse to comma separated string 
 speakers_ages <- xml_find_all(parsed_data, "//person/age") %>% 
  html_text(trim=T) %>% 
  paste(collapse = ", ")
 ## make a df row from these pieces of data
 current_row <- data.frame(
  corpus = "TCOF1",
  filename = file_name,
  blank_place_tidy = "",
  place_country = place_country,
  place_region =place_region,
  place_city = place_city,
  responsables = resps,
  resume = resume,
  year_reg = year_enreg,
  duree_s = duree_enreg,
  qualite_son = qualite_son,
  anon_signal = anon_signal,
  type_interaction = type_interaction,
  secteur = secteur,
  milieu = milieu,
  locuteurs = speakers,
  situation = situation,
  nombre_personnes = num_loc,
  conteint_mineurs = speakers_ages,
  filepath = current_file
 )
 # reset the values after sent to df row
 rm(file_name, place_country, place_region, place_city, resps, resume, year_enreg, duree_enreg, qualite_son, type_interaction, secteur, milieu, speakers, situation, num_loc, speakers_ages, current_file)
 ## bind rows together and write file to holding
 holding_data <- rbind(holding_data, current_row)
 write_tsv(holding_data, "/Users/Data/TCOF1_holding.csv")
  
}
