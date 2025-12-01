# R script to extract recording and speaker metadata from TCOF XML files v2

#### create dfs to hold info on enregistrements, locuteurs
all_enreg_data<- data.frame()
all_locuteur_data <- data.frame()

# helper function
col_to_commalist <- function(x) {
 paste(x, collapse = ", ")
}


# csv file containing paths to local XML files : same file used to download XML 
target_files <- read.csv('/Users/Data/target_files.csv', header=F)

names(target_files) <- "path"
# now run through files
#test_file <- "/Users/Data/etudiantes_jer_13.xml"
#file2 <- '/Users/Data/ANR_PREFAB/Corpus_ortolang/tcof/12/Corpus/Adultes/Conversations/film_tre_15/film_tre_15.xml'

# iterating over the list of files::
for (i in 1:dim(target_files)[1]){
 # read the html
 html_source <- read_html(target_files$path[i])
 
 ## get enreg_data : name_file = name of file, and then tidy
 name_file <- html_source %>% 
  html_elements(xpath = "//nomdossier") %>% 
  html_text()
 name_file <- gsub("/Verifie_Anonymise/", "", name_file)
 name_file <- gsub("../Corpus/", "", name_file)

 ## get number of speakers  
 nombre_locuteurs <- html_source %>% 
  html_elements(xpath = "//nombre_locuteurs") %>% 
  html_text()

 ## get type of relation between speakers as specified in XML
 relation_loc <- html_source %>% 
  html_elements(xpath = "//relation") %>% 
  html_text()

 # get resumé from XML field
 resume <- html_source %>% 
  html_elements(xpath = "//resume") %>% 
  html_text()
 if (length(resume) >1){resume <- resume[1]}
 
 # get name of transcription file
 tx_name <- html_source %>% 
  html_elements(xpath = "//transcription/nom_fichier") %>% 
  html_text()
 
 # put these pieces of data into a dataframe with 1 row = 1 recording
 enreg_data <- data.frame(
  name_file =name_file,
  nombre_locuteurs = nombre_locuteurs,
  relation_loc = relation_loc,
  resume = resume,
  tx_name = tx_name)
 
 
 # 
 # iterate over data for speakers
 # create df to hold results
 locuteur_dataAll <- data.frame()
 # specify xpath
 loc_xpath = "//locuteur[1]"
 
 # get chunk for speaker 
 this_locuteur_chunk <- html_source %>% 
  html_elements(xpath = loc_xpath) 

 # within this chunk, get identifier
 locuteur_id <- this_locuteur_chunk %>% 
  html_elements(xpath = paste0(loc_xpath , "//@identifiant")) %>% 
  html_text()
 # within this chunk, get age
 locuteur_age <- this_locuteur_chunk %>% 
  html_elements(xpath = paste0(loc_xpath , "//age")) %>% 
  html_text()
 # within this chunk, get gender
 locuteur_sexe <- this_locuteur_chunk %>% 
  html_elements(xpath = paste0(loc_xpath , "//sexe")) %>% 
  html_text()
 # within this chunk, get level of education
 locuteur_etudes <- this_locuteur_chunk %>% 
  html_elements(xpath = paste0(loc_xpath , "//etude")) %>% 
  html_text()
 # within this chunk, get whether french = native language
 statut_fr <- this_locuteur_chunk %>% 
  html_elements(xpath = paste0(loc_xpath , "//statut_francais")) %>% 
  html_text()
 # within this chunk, get place of birth
 lieu_ne <- this_locuteur_chunk %>% 
  html_elements(xpath = paste0(loc_xpath , "//naissance")) %>% 
  html_text()
 
 # combine data for speakers into df with 1 row = 1 person
 locuteur_dataAll <- data.frame(
  locuteur_id = locuteur_id,
  locuteur_age = locuteur_age,
  locuteur_sexe = locuteur_sexe,
  locuteur_etudes = locuteur_etudes,
  statut_fr = statut_fr,
  lieu_ne = lieu_ne ,
  name_file = name_file)
 # concatenate name of file in which person appears, -- and id to give full locuteur ID 
 locuteur_dataAll$locUUID <- paste0(locuteur_dataAll$name_file, "--",locuteur_dataAll$locuteur_id)
 # use custom function to collapse all locuteurUUIDs into string separated by , and add to column for corresponding recording
 enreg_data$loc_list <- col_to_commalist(locuteur_dataAll$locUUID)
 # combine these rows for recordings
 all_enreg_data <- rbind(all_enreg_data, enreg_data)
 # combine these rows for speakers
 all_locuteur_data <- rbind(all_locuteur_data, locuteur_dataAll)
 print(cat(i, "\n"))
}
# viewing and saving
View(all_locuteur_data)

write_csv(all_locuteur_data, "/Users/Data/ANR_PREFAB/Data/Corpus/MetadataCompilations/TCOF_locuteurs.csv")
write_csv(all_enreg_data, "/Users/Data/ANR_PREFAB/Data/Corpus/MetadataCompilations/TCOF_enreg.csv")
all_locuteur_data_sub <- all_locuteur_data[c(3, 8)]
write_csv(all_locuteur_data_sub, "/Users/Data/ANR_PREFAB/Data/Corpus/MetadataCompilations/TCOF_locuteurs_subset.csv")
