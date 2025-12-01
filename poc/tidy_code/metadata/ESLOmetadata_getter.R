# ESLO metadata
#1. get metadata files from eslo2 source page

ENTsourcepage <- "http://eslo.huma-num.fr/CorpusEslo/html/corpus/requete?type=C"

# faster to copy+paste this element and rearrange, export to csv::
#'//*[@id="idDivResult"]/div/div/div/div/div/table'

# 2. import list with Link, name as cols
links_to_get <- read_csv("/Users/Data/get_these_ESLOmeta2.csv")

# loop over files to dl all meta files
for (i in 1:dim(links_to_get)[1]){
 savename <- paste0("/Users/Data/ANR_PREFAB/Data/Metadata/ESLO2/", links_to_get$name[i])
 download_html(links_to_get$Link[i] , savename)
 cat("Processed file ", i, "of ", dim(links_to_get)[1], "\n")
 Sys.sleep(sample(10,1)/10)
}


# 3. create holding frame, list of files to process, remaps to do
all_meta <- tibble()
# get list of all files
eslo_files <- list.files(path = "/Users/Data/ANR_PREFAB/Data/Metadata/ESLO2/", full.names = T)
# set equivalences between values
qualite_son_values <- c("Mauvaise"="Bruité",
            "Passable" = "Peu bruité",
            "Excellente" = "Correcte",
            "Bonne" = "Correcte")
type_values <- c("_24H_"="Conversation",
         "_BOUL_"="Transaction",
         "_ECOLE_"="_a_verifier_",
         "_RUMEUR_"="Entretien",
         "_ENT_"="Entretien",
         "_CINE_"="Entretien",
         "_CONF_"="Cours",
         "_DIA_"="Entretien",
         "_ENTJEUN_"="Entretien",
         "_ITI_"="Conversation",
         "_INTPERS_"="Entretien",
         "_MEDIA_"="Médias oraux",
         "_REPAS_"="Conversation")

situation_values <- c("_24H_"="TV",
          "_BOUL_"="En public???",
          "_ECOLE_"="Face à face",
          "_RUMEUR_"="Face à face",
          "_ENT_"="Face à face",
          "_CINE_"="Face à face",
          "_CONF_"="En public",
          "_DIA_"="Face à face???",
          "_ENTJEUN_"="Face à face",
          "_ITI_"="Face à face",
          "_INTPERS_"="Face à face???",
          "_MEDIA_"="Radio???",
          "_REPAS_"="???")

milieu_values <-   c("_24H_"="??",
            "_BOUL_"="Commercial",
            "_ECOLE_"="??",
            "_RUMEUR_"="??",
            "_ENT_"="???",
            "_CINE_"="???",
            "_CONF_"="Académique",
            "_DIA_"="???",
            "_ENTJEUN_"="informatif??",
            "_ITI_"="informatif",
            "_INTPERS_"="divertissement???",
            "_MEDIA_"="informatif??",
            "_REPAS_"="Amical?Familial?")

secteur_values <-   c("_24H_"="Professionnel",
            "_BOUL_"="Professionnel",
            "_ECOLE_"="Privé",
            "_RUMEUR_"="Privé",
            "_ENT_"="Privé",
            "_CINE_"="Privé",
            "_CONF_"="Professionnel",
            "_DIA_"="Privé",
            "_ENTJEUN_"="Privé",
            "_ITI_"="Privé",
            "_INTPERS_"="Professionnel",
            "_MEDIA_"="Professionnel",
            "_REPAS_"="Privé")

# loop over files
for (f in 1:length(eslo_files))
# open loop here : read file, send to HTMLdoc
{html_document <- read_html(eslo_files[f])
#
niveaux_annotation <- "Morphosyntaxique, lemmatisation, analyse en dépendances"
annotation_est <- "Automatique"
modalite <- "Orale"
anon_signal <- '_a_verifier'
responsable <- ""
#liens_prealables <- "SANS"

# get category
categorie <- html_document %>% 
 html_elements(xpath = "//table//th[contains(text(), 'Catégorie')]/following-sibling::td") %>% 
 html_text()
if (categorie == "Entretien"){type_interation <- "Entretien"}

# get filename from name of audio file, tidy
nom_fichier <- html_document %>% 
 html_elements(xpath = "//table//th[contains(text(), 'Fichier son')]/following-sibling::td") %>% 
 html_text()
nom_fichier <- gsub(".wav", "", nom_fichier)

# get resumé of interation
resume <- html_document %>% 
 html_elements(xpath = "//table//th[contains(text(), 'catégorie')]/following-sibling::td") %>% 
 html_text()

# get type_raw from caption : will be used to infer type, situation, milieu and secteur with remaps
type_raw <- html_document %>% 
 html_elements(xpath = "//caption") %>% 
 html_text()
type_raw <- gsub("Référence enregistrement: ", "", type_raw)
type_raw <- substr(type_raw, 6, str_length(type_raw)-4)

# apply remaps, extracting as characters
type_interation <- as.character(type_values[type_raw])
situation_enreg <- as.character(situation_values[type_raw])
milieu <- as.character(milieu_values[type_raw])
secteur <- as.character(secteur_values[type_raw])

# get date
date_enregistrement <- html_document %>% 
 html_elements(xpath = "//table//th[contains(text(), 'enregistrement')]/following-sibling::td") %>% 
 html_text()

# get nature_signal
nature_signal <- html_document %>% 
 html_elements(xpath = "//table//th[contains(text(), 'Format:')]/following-sibling::td") %>% 
 html_text()

# get sound quality raw value, then remap
qualite_son_raw <- html_document %>% 
 html_elements(xpath = "//table//th[contains(text(), 'Acoustique')]/following-sibling::td") %>% 
 html_text()
qualite_son <- as.character(qualite_son_values[qualite_son_raw])

# get recording length
duree_enreg <- html_document %>% 
 html_elements(xpath = "//table//th[contains(text(), 'Durée')]/following-sibling::td") %>% 
 html_text()

#get address of metadata souce of current metadata
adresse_meta <- html_document %>% 
 html_elements(xpath = "//li/a[contains(text(), 'métadonnées')]/@href") %>% 
 html_text()

# get URL of audio
adresse_audio <- html_document %>% 
 html_elements(xpath = "//li/a[contains(text(), 'fichier sonore')]/@href") %>% 
 html_text()

# get list of speakers, including researchers
locuteurs_raw <- html_document %>% 
 html_elements(xpath = "//table//th[contains(text(), 'Locuteurs')]/following-sibling::td/ul/li/a") %>% 
 html_text()

# remove researchers from speakers, and convert from list to char string
locuteurs <- locuteurs_raw[!(str_starts(locuteurs_raw, "ch_"))]
locuteurs_tidy <-"" 
 for (i in locuteurs){
 locuteurs_tidy <- paste0(locuteurs_tidy, ", ", i )} 
locuteurs_tidy <- gsub("^, ","",locuteurs_tidy)
 

# if number of speakers is over 2, remap to 2+
nombre_locuteurs <- length(locuteurs)
 if (nombre_locuteurs > 2){ nombre_locuteurs <- "2+" } 

# create dataframe with data extracted : column_Name = value to be used in column
current_meta <- data.frame(
 nom_fichier = nom_fichier,
 responsable = responsable,
 resume = resume,
 date_enregistrement = date_enregistrement,
 duree_enregistrement = duree_enreg,
 qualite_son = qualite_son,
 anon_signal = anon_signal,
 niveaux_annotation = niveaux_annotation,
 annotation = annotation_est,
 type_interation = type_interation,
 secteur = secteur,
 milieu = milieu,
 modalite = modalite,
 locuteurs = locuteurs_tidy,
 liens_prealables = liens_prealables,
 nombre_locuteurs = nombre_locuteurs,
 adresse_audio = adresse_audio,
 adresse_meta = adresse_meta
)

# concatenate rows
all_meta <- rbind(all_meta, current_meta)
}
# when done, write df to csv
write.csv(all_meta, "/Users/Data/ANR_PREFAB/Data/Metadata/ESLO2auto-meta.csv")
# view metadata
View(all_meta)
######

###### done !
######

