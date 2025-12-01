
###### ESLO2 people metadata getter :: does this take the long way to get people data (recording ->name -> UUID -> person_meta) rather than (recording ->UUID ->person_meta : question of which XML file is taken as input as one has the UUIDs!)
native_status <- html_source %>% 
 html_elements(xpath = "//listperson/person/note[contains(@type, 'dialect')]") %>% 
 html_text()

# get tei_corpo files : get corpus file + internal codes + UUID for speakers present in corpus
test_file <- "/Users/Data/ANR_PREFAB/Corpus_ortolang/eslo/3/ESLO2_ENT/ESLO2_ENT_1019/ESLO2_ENT_1019_C.tei_corpo.xml"

this_path = '/Users/Data/ANR_PREFAB/Corpus_ortolang/eslo/3/'
test_files_eslo <- list.files(path = this_path, pattern=".tei_corpo.xml", full.names = T, recursive = T)
all_eslo_speakers <- data.frame()

for (i in 1:length(test_files_eslo)){
 this_file <- test_files_eslo[i]
 if (length(grep("ESLO2", this_file)==1)>0){

 html_source <- read_html(this_file)
# file.path(test_files_eslo[2])
 file_short <- gsub(this_path, "", this_file)

internal_codes <- html_source %>% 
 html_elements(xpath = "//listperson/person/altgrp/alt/@type") %>% 
 html_text()

 UUIDs <- html_source %>% 
 html_elements(xpath = "//listperson/person/persname") %>% 
 html_text()
 this_data <- data.frame(
 int_code = internal_codes,
 UUIDs = UUIDs,
 file_short = file_short
 )

 all_eslo_speakers <- rbind(all_eslo_speakers, this_data)
 if (i %%25 ==0){print(i)}
}

 }
 ## when have list of all UUIDs, need to iterate over 
## #http://eslo.huma-num.fr/CorpusEslo/html/fiche/locuteur?id=64
## etc to get information on 

tidy_UUIDs <- unique(all_eslo_speakers$UUIDs)
tidy_UUIDs <- as_tibble(tidy_UUIDs)
codes_eslo_speakers <- read.csv("/Users/Data//eslo_locuteur_codes.csv", sep="\t", header=F)
names(codes_eslo_speakers)[1] <- "URL"
names(codes_eslo_speakers)[2] <- "UUID"
names(tidy_UUIDs)[1] <- "UUID"
this_UUID <- tidy_UUIDs[23]
joined_data <- left_join(tidy_UUIDs, codes_eslo_speakers, by = "UUID")
joined_data$savename <- paste0(joined_data$UUID, ".xml")
save_path <- "/Users/Data/ANR_PREFAB/Corpus_ortolang/eslo/meta/"
for (i in 1:dim(joined_data)[1]){
  save_full <- paste0(save_path, joined_data$savename[i])
  try(download.file(joined_data$URL[i], save_full))
  Sys.sleep(10)  
 }
 
} 
# df to store all, retults
meta_eslo_all <- data.frame()

# files to process
eslo_people_meta_detail_files <- list.files(path = '/Users/Data/ANR_PREFAB/Corpus_ortolang/eslo/meta/', pattern = ".xml", full.names = T, recursive = T)

target_file <- eslo_people_meta_detail_files[1]
meta_all_person

build_people_meta_eslo <- function(target_file, meta_eslo_all){
 html_document <- read_html(target_file)

 UUIDraw <- html_document %>% 
 html_elements(xpath = "//caption") %>% 
 html_text()
 UUID <- gsub("Identifiant locuteur : ", "", UUIDraw)


 meta_type <- html_document %>% 
  html_elements(xpath = "//tr//*[contains(@class, 'ficheLabel' )]") %>% 
  html_text()
 meta_values <- html_document %>% 
  html_elements(xpath = "//tr//*[contains(@class, 'ficheValue' )]") %>% 
  html_text()

 meta_all_person <- data.frame(t(meta_values))
 names(meta_all_person) <- meta_type
 meta_all_person$UUID <- UUID
 meta_eslo_all <- rbind(meta_eslo_all, meta_all_person)
 
 return(meta_eslo_all)
}        

for (f in 1:length(eslo_people_meta_detail_files)){
 target_file <- eslo_people_meta_detail_files[f]
 meta_eslo_all <- build_people_meta_eslo(target_file, meta_eslo_all)
 if (f%%25 ==0){cat(f)}
}

# remove cols not needed
names(meta_eslo_all)
meta_eslo_all_strict <- meta_eslo_all[-c(7,8,9,10, 13:20)]
meta_eslo_all_strict$`Enregistrements et transcriptions:` <- gsub("Transcription ESLO2.+", "", meta_eslo_all_strict$`Enregistrements et transcriptions:`)
meta_eslo_all_strict$`Enregistrements et transcriptions:` <- gsub("Enregistrement ", "", meta_eslo_all_strict$`Enregistrements et transcriptions:`)
names(all_eslo_speakers)[2] <- "UUID"
all_eslo_speakers_bound <- left_join(all_eslo_speakers, meta_eslo_all_strict, by = "UUID")
write_csv(all_eslo_speakers_bound, "/Users/Data/ANR_PREFAB/Data/Corpus/MetadataCompilations/eslo2speakerdata.csv")

