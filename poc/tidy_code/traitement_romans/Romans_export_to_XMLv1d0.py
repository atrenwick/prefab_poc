## don't run ; this is v1.0
## this script runs in two parts : 
## Part 1 inserts conllu annotations into XML and exports to file. Specific writer-loops are present for PhraseoRom texts and for aligned DE-FR texts.
## Part 2 takes the exported XML files and gets the date, title, author and wordcount of each novel/ouvrage within the XML files and makes an Excel document, to easily verify that all required texts are present


# imports
import pandas as pd
import platform, re, json, glob, io, os, stanza, conllu, glob
import numpy as np 
from tqdm import tqdm
from stanza.utils.conll import CoNLL
from conllu import parse, parse_tree
from lxml import etree

def import_prepare_for_stanza(input_file):
  '''
  take a conllu file and load as stanza conllu doc
  Args : 
    input_file : path to file to tag
  Returns:
    source_doc : stanza document

  '''
  source_doc = CoNLL.conll2doc(input_file)

  return source_doc

def get_wordcount(conll_data):
	"""
	use conllu data to get wordcount excluding punctuation
	Args : 
		Input : conll_data : conllu annotations of a doc
		return: wordcount as int
	Note : function not used in present version of script
	"""
  punct_count =0
  for sentence in conll_data.sentences:
    for token in sentence.tokens:
      if (token.to_conll_text().split('\t')[3]) =="PUNCT":
        punct_count+=1
  
  count_allwords = conll_data.num_tokens
  wordcount = count_allwords - punct_count
  #print(f"All_words == {count_allwords}")
  #print(f"All_punct == {punct_count}")
  #print(f"Wordcount == {wordcount}")
  return wordcount

# file with Prefab_Conllu == conllfile : parse as conllu

# list of dictionaries to map conllu files with codenames to XML files with full names
## TO DO : take this input as a table and sent to one dict
name_converterFY = {"FY01-7200tv8.conllu":"FY.fr.ALBERT.xml","FY02-7200tv8.conllu":"FY.fr.BOUSQUET.xml",
"FY03-7200tv8.conllu":"FY.fr.BRASEY.xml",
"FY04-7200tv8.conllu":"FY.fr.BUJOR.xml",
"FY05-7200tv8.conllu":"FY.fr.DAMASIO.xml",
"FY06-7200tv8.conllu":"FY.fr.DARIO.xml",
"FY07-7200tv8.conllu":"FY.fr.DAY~BELLAGAMBA.xml",
"FY08-7200tv8.conllu":"FY.fr.DUCLOS.xml",
"FY09-7200tv8.conllu":"FY.fr.DUFOUR.xml",
"FY10-7200tv8.conllu":"FY.fr.JUBERT.xml",
"FY11-7200tv8.conllu":"FY.fr.KLOETZER.xml",
"FY12-7200tv8.conllu":"FY.fr.LOEVENBRUCK.xml",
"FY13-7200tv8.conllu":"FY.fr.MARCHIKA.xml",
"FY14-7200tv8.conllu":"FY.fr.ROBERT~GRIMBERT.xml",
"FY15-7200tv8.conllu":"FY.fr.SEGURA.xml",
"FY16-7200tv8.conllu":"FY.fr.TAFFIN.xml",
"FY17-7200tv8.conllu":"FY.fr.TIREL.xml",
"FY18-7200tv8.conllu":"FY.fr.VILA.xml",
"FY19-7200tv8.conllu":"FY.fr.BERTHELOT.xml",
"FY20-7200tv8.conllu":"FY.fr.CLAVEL2.xml",
"FY21-7200tv8.conllu":"FY.fr.DEBIEN.xml",
"FY22-7200tv8.conllu":"FY.fr.GIDON.xml",
"FY23-7200tv8.conllu":"FY.fr.GRIMBERT2.xml",
"FY24-7200tv8.conllu":"FY.fr.HELIOT.xml",
"FY25-7200tv8.conllu":"FY.fr.JAWORSKI.xml",
"FY26-7200tv8.conllu":"FY.fr.NOIREZ.xml",
"FY27-7200tv8.conllu":"FY.fr.THOMAS3.xml",
"FY28-7200tv8.conllu":"FY.fr.TOMAS.xml",
"FY29-7200tv8.conllu":"FY.fr.ANGE.xml",
"FY30-7200tv8.conllu":"FY.fr.BOISSET.xml",
"FY31-7200tv8.conllu":"FY.fr.BOTTERO.xml",
"FY32-7200tv8.conllu":"FY.fr.CAILLET.xml",
"FY33-7200tv8.conllu":"FY.fr.DAY.xml",
"FY34-7200tv8.conllu":"FY.fr.GODDYN.xml",
"FY35-7200tv8.conllu":"FY.fr.NIOGRET.xml",
"FY36-7200tv8.conllu":"FY.fr.CHATTAM.xml",
"FY37-7200tv8.conllu":"FY.fr.GABORIT.xml",
"FY38-7200tv8.conllu":"FY.fr.KATZ.xml",
"FY39-7200tv8.conllu":"FY.fr.PERU.xml",
"FY40-7200tv8.conllu":"FY.fr.FETJAINE.xml",
"FY41-7200tv8.conllu":"FY.fr.PEVEL.xml",
"FY42-7200tv8.conllu":"FY.fr.ROBERT.xml",
"FY43-7200tv8.conllu":"FY.fr.ROBILLARD.xml"}
nameconverterHS = {"HS01-7200tv8.conllu":"HIST.fr.BINET.xml",
"HS02-7200tv8.conllu":"HIST.fr.BRUSSOLO.xml",
"HS03-7200tv8.conllu":"HIST.fr.CHANDERNAGOR.xml",
"HS04-7200tv8.conllu":"HIST.fr.COMBESCOT.xml",
"HS05-7200tv8.conllu":"HIST.fr.DONNER.xml",
"HS06-7200tv8.conllu":"HIST.fr.DUPONT-MONOD.xml",
"HS07-7200tv8.conllu":"HIST.fr.FAYE2.xml",
"HS08-7200tv8.conllu":"HIST.fr.GAUDE.xml",
"HS09-7200tv8.conllu":"HIST.fr.GRAINVILLE.xml",
"HS10-7200tv8.conllu":"HIST.fr.HELIOT.xml",
"HS11-7200tv8.conllu":"HIST.fr.LEMAITRE.xml",
"HS12-7200tv8.conllu":"HIST.fr.MICHON.xml",
"HS13-7200tv8.conllu":"HIST.fr.MINARD.xml",
"HS14-7200tv8.conllu":"HIST.fr.NOIR.xml",
"HS15-7200tv8.conllu":"HIST.fr.ORSENNA.xml",
"HS16-7200tv8.conllu":"HIST.fr.SAGAN.xml",
"HS17-7200tv8.conllu":"HIST.fr.SIGNOL.xml",
"HS18-7200tv8.conllu":"HIST.fr.TAILLANDIER.xml",
"HS19-7200tv8.conllu":"HIST.fr.THOMAS.xml",
"HS20-7200tv8.conllu":"HIST.fr.TOURNIER.xml",
"HS21-7200tv8.conllu":"HIST.fr.VAUTRIN.xml",
"HS22-7200tv8.conllu":"HIST.fr.BODARD.xml",
"HS23-7200tv8.conllu":"HIST.fr.GALLO.xml",
"HS24-7200tv8.conllu":"HIST.fr.MAKINE.xml",
"HS25-7200tv8.conllu":"HIST.fr.SCHMITT.xml",
"HS26-7200tv8.conllu":"HIST.fr.TROYAT.xml",
"HS27-7200tv8.conllu":"HIST.fr.CAMUS2.xml",
"HS28-7200tv8.conllu":"HIST.fr.D'AILLON.xml",
"HS29-7200tv8.conllu":"HIST.fr.RUFIN.xml",
"HS30-7200tv8.conllu":"HIST.fr.FAJARDIE.xml",
"HS31-7200tv8.conllu":"HIST.fr.LAINE.xml",
"HS32-7200tv8.conllu":"HIST.fr.RAMBAUD.xml",
"HS33-7200tv8.conllu":"HIST.fr.GIRARD.xml",
"HS34-7200tv8.conllu":"HIST.fr.CLAVEL.xml",
"HS35-7200tv8.conllu":"HIST.fr.JACQ.xml",
"HS36-7200tv8.conllu":"HIST.fr.MERLE.xml",
"HS36-7200tv8.conllu":"HIST.fr.MERLE.xml"}
name_converterPO = {"PO01-7200tv8.conllu":"POL.fr.ARNAUD.xml",
"PO02-7200tv8.conllu":"POL.fr.AUDIN.xml",
"PO03-7200tv8.conllu":"POL.fr.AZINCOURT.xml",
"PO04-7200tv8.conllu":"POL.fr.BANNEL.xml",
"PO05-7200tv8.conllu":"POL.fr.BAPTISTE-MARREY.xml",
"PO06-7200tv8.conllu":"POL.fr.BELIN.xml",
"PO07-7200tv8.conllu":"POL.fr.BELLETTO.xml",
"PO08-7200tv8.conllu":"POL.fr.BESSON2.xml",
"PO09-7200tv8.conllu":"POL.fr.BOILEAU~NARCEJAC.xml",
"PO10-7200tv8.conllu":"POL.fr.BORROMEE.xml",
"PO11-7200tv8.conllu":"POL.fr.BRUCKNER.xml",
"PO12-7200tv8.conllu":"POL.fr.BUSSI.xml",
"PO13-7200tv8.conllu":"POL.fr.CAMUT~HUG.xml",
"PO14-7200tv8.conllu":"POL.fr.CHAINAS.xml",
"PO15-7200tv8.conllu":"POL.fr.CHAMOISEAU.xml",
"PO16-7200tv8.conllu":"POL.fr.CLAUDEL.xml",
"PO17-7200tv8.conllu":"POL.fr.CRESPY.xml",
"PO18-7200tv8.conllu":"POL.fr.DELAFOSSE.xml",
"PO19-7200tv8.conllu":"POL.fr.DELTEIL.xml",
"PO20-7200tv8.conllu":"POL.fr.DICKER.xml",
"PO21-7200tv8.conllu":"POL.fr.DOA.xml",
"PO23-7200tv8.conllu":"POL.fr.FRITSCH.xml",
"PO24-7200tv8.conllu":"POL.fr.GALLO.xml",
"PO25-7200tv8.conllu":"POL.fr.GIESBERT.xml",
"PO26-7200tv8.conllu":"POL.fr.GRASSET.xml",
"PO27-7200tv8.conllu":"POL.fr.GUILLAUMOT.xml",
"PO28-7200tv8.conllu":"POL.fr.HALTER.xml",
"PO29-7200tv8.conllu":"POL.fr.JARRIGE.xml",
"PO30-7200tv8.conllu":"POL.fr.JONCOUR.xml",
"PO31-7200tv8.conllu":"POL.fr.KHADRA.xml",
"PO32-7200tv8.conllu":"POL.fr.KLOPMANN.xml",
"PO33-7200tv8.conllu":"POL.fr.LABERGE.xml",
"PO34-7200tv8.conllu":"POL.fr.LANGLOIS.xml",
"PO35-7200tv8.conllu":"POL.fr.LEVY2.xml",
"PO36-7200tv8.conllu":"POL.fr.MACOUIN.xml",
"PO37-7200tv8.conllu":"POL.fr.MANCHETTE.xml",
"PO38-7200tv8.conllu":"POL.fr.MAUWLS.xml",
"PO39-7200tv8.conllu":"POL.fr.MICHAUD.xml",
"PO40-7200tv8.conllu":"POL.fr.MOLAY.xml",
"PO41-7200tv8.conllu":"POL.fr.MORILLON.xml",
"PO42-7200tv8.conllu":"POL.fr.MUSSO2.xml",
"PO43-7200tv8.conllu":"POL.fr.NOIREZ.xml",
"PO44-7200tv8.conllu":"POL.fr.OLIVAUX.xml",
"PO45-7200tv8.conllu":"POL.fr.PAROT.xml",
"PO46-7200tv8.conllu":"POL.fr.PEREC.xml",
"PO47-7200tv8.conllu":"POL.fr.RAGON.xml",
"PO48-7200tv8.conllu":"POL.fr.RUFIN.xml",
"PO49-7200tv8.conllu":"POL.fr.STAROSTA.xml",
"PO50-7200tv8.conllu":"POL.fr.SYLVAIN.xml",
"PO51-7200tv8.conllu":"POL.fr.THILLIEZ.xml",
"PO52-7200tv8.conllu":"POL.fr.VANCAUWELAERT.xml",
"PO53-7200tv8.conllu":"POL.fr.AUBERT.xml",
"PO54-7200tv8.conllu":"POL.fr.COLLETTE.xml",
"PO55-7200tv8.conllu":"POL.fr.DELAURE.xml",
"PO56-7200tv8.conllu":"POL.fr.GAY.xml",
"PO57-7200tv8.conllu":"POL.fr.GIEBEL.xml",
"PO58-7200tv8.conllu":"POL.fr.LEMAITRE.xml",
"PO59-7200tv8.conllu":"POL.fr.MAGNAN.xml",
"PO60-7200tv8.conllu":"POL.fr.SENECAL.xml",
"PO61-7200tv8.conllu":"POL.fr.VILLARD.xml",
"PO62-7200tv8.conllu":"POL.fr.VINCENT.xml",
"PO63-7200tv8.conllu":"POL.fr.BESSON.xml",
"PO64-7200tv8.conllu":"POL.fr.DANTEC.xml",
"PO65-7200tv8.conllu":"POL.fr.DARD.xml",
"PO66-7200tv8.conllu":"POL.fr.DEVILLIERS.xml",
"PO67-7200tv8.conllu":"POL.fr.IZZO.xml",
"PO68-7200tv8.conllu":"POL.fr.LENTERIC.xml",
"PO69-7200tv8.conllu":"POL.fr.PENNAC.xml",
"PO70-7200tv8.conllu":"POL.fr.THIERY.xml",
"PO71-7200tv8.conllu":"POL.fr.VAUTRIN.xml",
"PO72-7200tv8.conllu":"POL.fr.PAROT.xml",
"PO73-7200tv8.conllu":"POL.fr.DAENINCKX.xml",
"PO74-7200tv8.conllu":"POL.fr.CHATTAM.xml",
"PO75-7200tv8.conllu":"POL.fr.GRANGE.xml",
"PO76-7200tv8.conllu":"POL.fr.BROUILLET.xml",
"PO77-7200tv8.conllu":"POL.fr.JAPP.xml",
"PO78-7200tv8.conllu":"POL.fr.POUY.xml",
"PO79-7200tv8.conllu":"POL.fr.JONQUET.xml",
"PO80-7200tv8.conllu":"POL.fr.VARGAS.xml",
"PO81-7200tv8.conllu":"POL.fr.BRUSSOLO.xml",
"PO99-7200v3tv8.conllu":"POL.fr.OPPEL.xml"
}
name_converterSF= {
"SF01-7200tv8.conllu":"SF.fr.BARBERI.xml",
"SF02-7200tv8.conllu":"SF.fr.DAY.xml",
"SF03-7200tv8.conllu":"SF.fr.DUFOUR.xml",
"SF04-7200tv8.conllu":"SF.fr.HERAULT.xml",
"SF05-7200tv8.conllu":"SF.fr.JEURY.xml",
"SF06-7200tv8.conllu":"SF.fr.LEFEBVRE.xml",
"SF07-7200tv8.conllu":"SF.fr.LIGNY~GOULT.xml",
"SF08-7200tv8.conllu":"SF.fr.PELOT.xml",
"SF09-7200tv8.conllu":"SF.fr.RUFIN.xml",
"SF10-7200tv8.conllu":"SF.fr.WINTREBERT.xml",
"SF11-7200tv8.conllu":"SF.fr.FONTANA.xml",
"SF12-7200tv8.conllu":"SF.fr.VANCAUWELAERT.xml",
"SF13-7200tv8.conllu":"SF.fr.VOLODINE.xml",
"SF14-7200tv8.conllu":"SF.fr.VONARBURG.xml",
"SF15-7200tv8.conllu":"SF.fr.BRUSSOLO.xml",
"SF16-7200tv8.conllu":"SF.fr.CANAL.xml",
"SF17-7200tv8.conllu":"SF.fr.DANTEC.xml",
"SF18-7200tv8.conllu":"SF.fr.LEHMAN.xml",
"SF19-7200tv8.conllu":"SF.fr.WAGNER.xml",
"SF20-7200tv8.conllu":"SF.fr.DIROLLO.xml",
"SF21-7200tv8.conllu":"SF.fr.GENEFORT.xml",
"SF22-7200tv8.conllu":"SF.fr.CURVAL.xml",
"SF23-7200tv8.conllu":"SF.fr.AYERDHAL.xml",
"SF24-7200tv8.conllu":"SF.fr.HELIOT.xml",
"SF25-7200tv8.conllu":"SF.fr.LIGNY.xml",
"SF26-7200tv8.conllu":"SF.fr.WALTHER.xml",
"SF27-7200tv8.conllu":"SF.fr.BORDAGE.xml",
"SF28-7200tv8.conllu":"SF.fr.WERBER.xml"
}
name_converterSE = {
"SN01-7200tv8.conllu":"SENT.fr.ARSAND.xml",
"SN02-7200tv8.conllu":"SENT.fr.BERNHEIM.xml",
"SN03-7200tv8.conllu":"SENT.fr.BREAU.xml",
"SN04-7200tv8.conllu":"SENT.fr.CAUVIN.xml",
"SN05-7200tv8.conllu":"SENT.fr.DELACOURT.xml",
"SN06-7200tv8.conllu":"SENT.fr.FALK.xml",
"SN07-7200tv8.conllu":"SENT.fr.GALLAY.xml",
"SN08-7200tv8.conllu":"SENT.fr.HENRIONNET.xml",
"SN09-7200tv8.conllu":"SENT.fr.INGUIMBERT.xml",
"SN10-7200tv8.conllu":"SENT.fr.JOSSE.xml",
"SN11-7200tv8.conllu":"SENT.fr.KESTEMAN.xml",
"SN12-7200tv8.conllu":"SENT.fr.LABRECHE.xml",
"SN13-7200tv8.conllu":"SENT.fr.LAMBERT.xml",
"SN14-7200tv8.conllu":"SENT.fr.LEDIG.xml",
"SN15-7200tv8.conllu":"SENT.fr.LENOIR~GREGGIO.xml",
"SN16-7200tv8.conllu":"SENT.fr.LEVY3.xml",
"SN17-7200tv8.conllu":"SENT.fr.PACULA.xml",
"SN18-7200tv8.conllu":"SENT.fr.SIMART.xml",
"SN19-7200tv8.conllu":"SENT.fr.TEULIE.xml",
"SN20-7200tv8.conllu":"SENT.fr.ALEXIS.xml",
"SN21-7200tv8.conllu":"SENT.fr.CHATELET.xml",
"SN22-7200tv8.conllu":"SENT.fr.JARDIN.xml",
"SN23-7200tv8.conllu":"SENT.fr.JOMAIN.xml",
"SN24-7200tv8.conllu":"SENT.fr.TROMPETTE.xml",
"SN25-7200tv8.conllu":"SENT.fr.VAREILLE.xml",
"SN26-7200tv8.conllu":"SENT.fr.ABECASSIS.xml",
"SN27-7200tv8.conllu":"SENT.fr.GOLON~GOLON.xml",
"SN28-7200tv8.conllu":"SENT.fr.KERNEL.xml",
"SN29-7200tv8.conllu":"SENT.fr.LEGARDINIER.xml",
"SN30-7200tv8.conllu":"SENT.fr.OLMI.xml",
"SN31-7200tv8.conllu":"SENT.fr.VANCAUWELAERT.xml",
"SN32-7200tv8.conllu":"SENT.fr.PANCOL.xml",
"SN33-7200tv8.conllu":"SENT.fr.DEBURON.xml",
"SN34-7200tv8.conllu":"SENT.fr.GAVALDA.xml",
"SN35-7200tv8.conllu":"SENT.fr.MUSSO.xml",
"SN36-7200tv8.conllu":"SENT.fr.BOISSARD.xml",
"SN37-7200tv8.conllu":"SENT.fr.BOURDIN.xml",
"SN38-7200tv8.conllu":"SENT.fr.LEVY2.xml"
}
name_converterGF = {
"GF01-7200tv8.conllu":"GEN.fr.ROUAUD.xml",
"GF02-7200tv8.conllu":"GEN.fr.SIMON.xml",
"GF03-7200tv8.conllu":"GEN.fr.TOURNIER.xml",
"GF04-7200tv8.conllu":"GEN.fr.TOUSSAINT.xml",
"GF05-7200tv8.conllu":"GEN.fr.CHESSEX.xml",
"GF06-7200tv8.conllu":"GEN.fr.CLAUDEL.xml",
"GF07-7200tv8.conllu":"GEN.fr.D'ORMESSON.xml",
"GF08-7200tv8.conllu":"GEN.fr.DEVILLE.xml",
"GF09-7200tv8.conllu":"GEN.fr.ERNAUX.xml",
"GF10-7200tv8.conllu":"GEN.fr.HUMBERT.xml",
"GF11-7200tv8.conllu":"GEN.fr.LAFERRIERE.xml",
"GF12-7200tv8.conllu":"GEN.fr.PENNAC.xml",
"GF13-7200tv8.conllu":"GEN.fr.QUEFFELEC.xml",
"GF14-7200tv8.conllu":"GEN.fr.ROZE.xml",
"GF15-7200tv8.conllu":"GEN.fr.BAZIN.xml",
"GF16-7200tv8.conllu":"GEN.fr.CINTAS.xml",
"GF17-7200tv8.conllu":"GEN.fr.DECOIN.xml",
"GF18-7200tv8.conllu":"GEN.fr.DURAS.xml",
"GF19-7200tv8.conllu":"GEN.fr.QUIGNARD.xml",
"GF20-7200tv8.conllu":"GEN.fr.VANCAUWELAERT.xml",
"GF21-7200tv8.conllu":"GEN.fr.BEIGBEDER.xml",
"GF22-7200tv8.conllu":"GEN.fr.BENJELLOUN.xml",
"GF23-7200tv8.conllu":"GEN.fr.KHADRA.xml",
"GF24-7200tv8.conllu":"GEN.fr.ORSENNA.xml",
"GF25-7200tv8.conllu":"GEN.fr.RUFIN.xml",
"GF26-7200tv8.conllu":"GEN.fr.SOLLERS.xml",
"GF27-7200tv8.conllu":"GEN.fr.HOUELLEBECQ.xml",
"GF28-7200tv8.conllu":"GEN.fr.LECLEZIO.xml",
"GF29-7200tv8.conllu":"GEN.fr.MAALOUF.xml",
"GF30-7200tv8.conllu":"GEN.fr.RAMBAUD.xml",
"GF31-7200tv8.conllu":"GEN.fr.CLAVEL.xml",
"GF32-7200tv8.conllu":"GEN.fr.FOENKINOS.xml",
"GF33-7200tv8.conllu":"GEN.fr.SCHMITT.xml",
"GF34-7200tv8.conllu":"GEN.fr.DJIAN.xml",
"GF35-7200tv8.conllu":"GEN.fr.ECHENOZ.xml",
"GF36-7200tv8.conllu":"GEN.fr.MILLET.xml",
"GF37-7200tv8.conllu":"GEN.fr.MODIANO.xml",
"GF38-7200tv8.conllu":"GEN.fr.NOTHOMB.xml"
}
name_converterGE = {
"GE01-7200tv8.conllu":"GEN.fr.ACHARD.xml",
"GE02-7200tv8.conllu":"GEN.fr.ADAM.xml",
"GE03-7200tv8.conllu":"GEN.fr.ANGOT.xml",
"GE04-7200tv8.conllu":"GEN.fr.AUTISSIER.xml",
"GE05-7200tv8.conllu":"GEN.fr.BARBERY.xml",
"GE06-7200tv8.conllu":"GEN.fr.BECK.xml",
"GE07-7200tv8.conllu":"GEN.fr.BENACQUISTA.xml",
"GE08-7200tv8.conllu":"GEN.fr.BENFODIL.xml",
"GE09-7200tv8.conllu":"GEN.fr.BOURGEADE.xml",
"GE10-7200tv8.conllu":"GEN.fr.BRISAC.xml",
"GE11-7200tv8.conllu":"GEN.fr.BROCAS.xml",
"GE12-7200tv8.conllu":"GEN.fr.CHAMOISEAU.xml",
"GE13-7200tv8.conllu":"GEN.fr.CHEVRIER.xml",
"GE14-7200tv8.conllu":"GEN.fr.COMBESCOT.xml",
"GE15-7200tv8.conllu":"GEN.fr.CONSTANT.xml",
"GE16-7200tv8.conllu":"GEN.fr.COSSE.xml",
"GE17-7200tv8.conllu":"GEN.fr.DAOUD.xml",
"GE18-7200tv8.conllu":"GEN.fr.DEFALVARD.xml",
"GE19-7200tv8.conllu":"GEN.fr.DEFORGES.xml",
"GE20-7200tv8.conllu":"GEN.fr.DEGHELT.xml",
"GE21-7200tv8.conllu":"GEN.fr.DELACOURT.xml",
"GE22-7200tv8.conllu":"GEN.fr.DELLESTABLE.xml",
"GE23-7200tv8.conllu":"GEN.fr.DELOFFRE.xml",
"GE24-7200tv8.conllu":"GEN.fr.DEON.xml",
"GE25-7200tv8.conllu":"GEN.fr.DEPESTRE.xml",
"GE26-7200tv8.conllu":"GEN.fr.DESESQUELLES.xml",
"GE27-7200tv8.conllu":"GEN.fr.DESPENTES.xml",
"GE28-7200tv8.conllu":"GEN.fr.DEVIGAN.xml",
"GE29-7200tv8.conllu":"GEN.fr.DONNER.xml",
"GE30-7200tv8.conllu":"GEN.fr.DUBOIS.xml",
"GE31-7200tv8.conllu":"GEN.fr.FABRE.xml",
"GE32-7200tv8.conllu":"GEN.fr.FAYE.xml",
"GE33-7200tv8.conllu":"GEN.fr.FERMINE.xml",
"GE34-7200tv8.conllu":"GEN.fr.GARY.xml",
"GE35-7200tv8.conllu":"GEN.fr.GOBY.xml",
"GE36-7200tv8.conllu":"GEN.fr.GRANNEC.xml",
"GE37-7200tv8.conllu":"GEN.fr.GRIMBERT.xml",
"GE38-7200tv8.conllu":"GEN.fr.HAENEL.xml",
"GE39-7200tv8.conllu":"GEN.fr.HUSTON.xml",
"GE40-7200tv8.conllu":"GEN.fr.JAOUEN.xml",
"GE41-7200tv8.conllu":"GEN.fr.JAPRISOT.xml",
"GE42-7200tv8.conllu":"GEN.fr.JEAN.xml",
"GE43-7200tv8.conllu":"GEN.fr.JENNI.xml",
"GE44-7200tv8.conllu":"GEN.fr.JOSSE.xml",
"GE45-7200tv8.conllu":"GEN.fr.KAPRIELIAN.xml",
"GE46-7200tv8.conllu":"GEN.fr.KOUROUMA.xml",
"GE47-7200tv8.conllu":"GEN.fr.LABERGE.xml",
"GE48-7200tv8.conllu":"GEN.fr.LAHENS.xml",
"GE49-7200tv8.conllu":"GEN.fr.LAROUI.xml",
"GE50-7200tv8.conllu":"GEN.fr.LEROY.xml",
"GE51-7200tv8.conllu":"GEN.fr.LITTELL.xml",
"GE52-7200tv8.conllu":"GEN.fr.MAILLET.xml",
"GE53-7200tv8.conllu":"GEN.fr.MERLE.xml",
"GE54-7200tv8.conllu":"GEN.fr.MINARD.xml",
"GE55-7200tv8.conllu":"GEN.fr.MINGARELLI.xml",
"GE56-7200tv8.conllu":"GEN.fr.MONENEMBO.xml",
"GE57-7200tv8.conllu":"GEN.fr.MUKASONGA.xml",
"GE58-7200tv8.conllu":"GEN.fr.NAVARRE.xml",
"GE59-7200tv8.conllu":"GEN.fr.PICOULY.xml",
"GE60-7200tv8.conllu":"GEN.fr.PIREYRE.xml",
"GE61-7200tv8.conllu":"GEN.fr.POSTEL.xml",
"GE62-7200tv8.conllu":"GEN.fr.RAHIMI.xml",
"GE63-7200tv8.conllu":"GEN.fr.REINHARDT.xml",
"GE64-7200tv8.conllu":"GEN.fr.REZA.xml",
"GE65-7200tv8.conllu":"GEN.fr.RINALDI.xml",
"GE66-7200tv8.conllu":"GEN.fr.RUBEN.xml",
"GE67-7200tv8.conllu":"GEN.fr.SABATIER.xml",
"GE68-7200tv8.conllu":"GEN.fr.SABOLO.xml",
"GE69-7200tv8.conllu":"GEN.fr.SAGAN.xml",
"GE70-7200tv8.conllu":"GEN.fr.SALVAYRE.xml",
"GE71-7200tv8.conllu":"GEN.fr.SANSAL.xml",
"GE72-7200tv8.conllu":"GEN.fr.SEYVOS.xml",
"GE73-7200tv8.conllu":"GEN.fr.SLIMANI.xml",
"GE74-7200tv8.conllu":"GEN.fr.SPORTES.xml",
"GE75-7200tv8.conllu":"GEN.fr.TROYAT.xml",
"GE76-7200tv8.conllu":"GEN.fr.VAUTRIN.xml",
"GE77-7200tv8.conllu":"GEN.fr.VOLODINE.xml",
"GE78-7200tv8.conllu":"GEN.fr.WEYERGANS.xml",
"GE79-7200tv8.conllu":"GEN.fr.BERGER2.xml",
"GE80-7200tv8.conllu":"GEN.fr.BOURAOUI.xml",
"GE81-7200tv8.conllu":"GEN.fr.CHALANDON.xml",
"GE82-7200tv8.conllu":"GEN.fr.CUSSET.xml",
"GE83-7200tv8.conllu":"GEN.fr.DARRIEUSSECQ.xml",
"GE84-7200tv8.conllu":"GEN.fr.DEKERANGAL.xml",
"GE85-7200tv8.conllu":"GEN.fr.DEMANDIARGUES.xml",
"GE86-7200tv8.conllu":"GEN.fr.DIVRY.xml",
"GE87-7200tv8.conllu":"GEN.fr.DUTEURTRE.xml",
"GE88-7200tv8.conllu":"GEN.fr.FERNANDEZ.xml",
"GE89-7200tv8.conllu":"GEN.fr.FERRARI.xml",
"GE90-7200tv8.conllu":"GEN.fr.GARDE.xml",
"GE91-7200tv8.conllu":"GEN.fr.GAUDE.xml",
"GE92-7200tv8.conllu":"GEN.fr.GRAINVILLE.xml",
"GE93-7200tv8.conllu":"GEN.fr.LAURENT.xml",
"GE94-7200tv8.conllu":"GEN.fr.MAUVIGNIER.xml",
"GE95-7200tv8.conllu":"GEN.fr.MICHON.xml",
"GE96-7200tv8.conllu":"GEN.fr.NDIAYE.xml",
"GE97-7200tv8.conllu":"GEN.fr.ONO-DIT-BIOT.xml",
"GE98-7200tv8.conllu":"GEN.fr.ROCHEFORT.xml",
"GE99-7200tv8.conllu":"GEN.fr.ROGER.xml"
}
name_converterNUM = {"162-7000tv8.conllu":"jean-christophe grange_les rivières pourpres.fr.xml",
"318-7000tv8.conllu":"michel houellebecq_extension du domaine de la lutte.fr.xml",
"322-7000tv8.conllu":"amélie nothomb_stupeur et tremblements.fr.xml",
"347-7000tv8.conllu":"bernard werber_fourmis 02 - le jour des fourmis.fr.xml",
"358-7000tv8.conllu":"marguerite duras_l'amant.fr.xml",
"416-7000tv8.conllu":"nicole de buron_chéri tu m'écoutes alors répète ce que je viens de dire….fr.xml",
"417-7000tv8.conllu":"jean-bernard pouy_la clef des mensonges.fr.xml",
"418-7000tv8.conllu":"françois cavanna_les yeux plus grands que le ventre.fr.xml",
"420-7000tv8.conllu":"benoîte groult_les trois quarts du temps.fr.xml",
"422-7000tv8.conllu":"jean-marie le clézio_désert.fr.xml",
"424-7000tv8.conllu":"suzanne prou_les amis de monsieur paul.fr.xml",
"426-7000tv8.conllu":"jean echenoz_je m'en vais.fr.xml",
"428-7000tv8.conllu":"philippe djian_372° le matin.fr.xml",
"432-7000tv8.conllu":"eric-emmanuel schmitt_monsieur ibrahim et les fleurs du coran.fr.xml",
"435-7000tv8.conllu":"jean canolle_la maison des esclaves.fr.xml",
"438-7000tv8.conllu":"paul-loup sulitzer_fortune.fr.xml",
"439-7000tv8.conllu":"denis belloc_néons.fr.xml",
"440-7000tv8.conllu":"andré roussin_la vie est trop courte.fr.xml",
"445-7000tv8.conllu":"brigitte aubert_descentes d'organes.fr.xml",
"448-7000tv8.conllu":"christian jacq_paneb l'ardent.fr.xml",
"450-7000tv8.conllu":"françoise giroud_mon très cher amour….fr.xml",
"451-7000tv8.conllu":"daniel picouly_nec.fr.xml",
"456-7000tv8.conllu":"bernard werber_fourmis 03 - la révolution des fourmis.fr.xml",
"460-7000tv8.conllu":"yann queffélec_les noces barbares.fr.xml",
"463-7000tv8.conllu":"fred vargas_debout les morts.fr.xml",
"464-7000tv8.conllu":"françoise dorner_la douceur assassine.fr.xml",
"466-7000tv8.conllu":"patrick modiano_accident nocturne.fr.xml",
"467-7000tv8.conllu":"philippe djian_sotos.fr.xml",
"470-7000tv8.conllu":"frédérique hébrard_esther mazel.fr.xml",
"471-7000tv8.conllu":"noëlle châtelet_la femme coquelicot.fr.xml",
"476-7000tv8.conllu":"brigitte sauzay_le vertige allemand.fr.xml",
"480-7000tv8.conllu":"geneviève dormann_la petite main.fr.xml",
"483-7000tv8.conllu":"anne delbé_une femme.fr.xml",
"484-7000tv8.conllu":"fred vargas_coule la seine.fr.xml",
"485-7000tv8.conllu":"françoise dorin_les vendanges tardives.fr.xml",
"491-7000tv8.conllu":"hélie de saint-marc_mémoires de braises.fr.xml",
"492-7000tv8.conllu":"annie ernaux_la place.fr.xml",
"495-7000tv8.conllu":"benoîte groult_les vaisseaux du coeur.fr.xml",
"496-7000tv8.conllu":"alain leblanc_un pont entre deux rives.fr.xml",
"497-7000tv8.conllu":"amélie nothomb_robert des noms propres.fr.xml",
"498-7000tv8.conllu":"jean-bernard pouy_la pêche aux anges.fr.xml",
"501-7000tv8.conllu":"jacqueline cauët_les filles du maître de chai.fr.xml",
"507-7000tv8.conllu":"pierre magnan_la maison assassinée.fr.xml",
"508-7000tv8.conllu":"nicole de buron_qui c'est ce garçon.fr.xml",
"515-7000tv8.conllu":"dominique lapierre_la cité de la joie.fr.xml",
"516-7000tv8.conllu":"jean-claude izzo_chourmo.fr.xml",
"517-7000tv8.conllu":"flora groult_le coup de la reine d'espagne.fr.xml",
"521-7000tv8.conllu":"dominique fernandez_dans la main de l'ange.fr.xml",
"523-7000tv8.conllu":"didier decoin_docile.fr.xml",
"524-7000tv8.conllu":"noëlle châtelet_la dame en bleu.fr.xml",
"527-7000tv8.conllu":"nicole de buron_mais t'as tout pour être heureuse.fr.xml",
"528-7000tv8.conllu":"jean-claude izzo_total khéops.fr.xml",
"532-7000tv8.conllu":"mehdi charef_le thé au harem d'archi ahmed.fr.xml",
"533-7000tv8.conllu":"valérie thérame_bastienne.fr.xml",
"536-7000tv8.conllu":"philippe stéphanie; labro_des cornichons au chocolat.fr.xml",
"539-7000tv8.conllu":"alexandre jardin_le zubial.fr.xml",
"541-7000tv8.conllu":"alain minc_la grande illusion.fr.xml",
"542-7000tv8.conllu":"janine boissard_marie-tempête.fr.xml",
"543-7000tv8.conllu":"françois lelord_à la poursuite du temps qui passe.fr.xml",
"546-7000tv8.conllu":"françoise sagan_un orage immobile.fr.xml",
"550-7000tv8.conllu":"noëlle châtelet_la petite aux tournesols.fr.xml",
"553-7000tv8.conllu":"fred vargas_l'homme à l'envers.fr.xml",
"556-7000tv8.conllu":"guy de maupassant_nouvelles choisies.fr.xml",
"558-7000tv8.conllu":"didier van cauwelaert_la vie interdite.fr.xml",
"561-7000tv8.conllu":"denis belloc_képas.fr.xml",
"570-7000tv8.conllu":"madeleine chapsal_envoyez la petite musique….fr.xml",
"571-7000tv8.conllu":"robert merle_le propre de l'homme.fr.xml",
"578-7000tv8.conllu":"michel tournier_la goutte d'or.fr.xml",
"579-7000tv8.conllu":"brigitte aubert_la mort des bois.fr.xml",
"580-7000tv8.conllu":"remo forlani_gouttière.fr.xml",
"582-7000tv8.conllu":"pierre; narcejac thomas boileau_champ clos.fr.xml",
"629-7000tv8.conllu":"marguerite yourcenar_conte bleu le premier soir maléfice.fr.xml",
"633-7000tv8.conllu":"jean-charles; dasquie guillaume brisard_ben laden - la vérité interdite.fr.xml",
"639-7000tv8.conllu":"françoise & grass günter  giroud_ecoutez-moi. paris-berlin aller-retour.fr.xml",
"641-7000tv8.conllu":"marguerite duras_la maladie de la mort.fr.xml",
"642-7000tv8.conllu":"françoise dorin_le tout pour le tout.fr.xml"
}
name_converterDTRS = {
"DF01-7200tv8.conllu":"george simenon_la chambre bleue.fr.xml",
"DF02-7200tv8.conllu":"george simenon_la colère de maigret.fr.xml",
"DF03-7200tv8.conllu":"george simenon_la marie du port.fr.xml",
"DF04-7200tv8.conllu":"george simenon_le chien jaune.fr.xml",
"DF05-7200tv8.conllu":"george simenon_le locataire.fr.xml",
"DF06-7200tv8.conllu":"george simenon_les anneaux de bicetre.fr.xml",
"DF07-7200tv8.conllu":"stendhal_le rouge et le noir.fr.xml",
"DF08-7200tv8.conllu":"gustave flaubert_l'éducation sentimentale.fr.xml",
"DF09-7200tv8.conllu":"jean giraudoux_la folle de chaillot.fr.xml",
"TR01-7200tv8.conllu":"jean cocteau_la machine infernale.fr.xml",
"TR02-7200tv8.conllu":"jean cocteau_thomas l'imposteur.fr.xml",
"TR03-7200tv8.conllu":"jean egen_les tilleuls de lautenbach. mémoires d'alsace.fr.xml",
"TR05-7200tv8.conllu":"jean giraudoux_la guerre de troie n'aura pas lieu.fr.xml",
"TR06-7200tv8.conllu":"jean hougron_soleil au ventre.fr.xml",
"TR07-7200tv8.conllu":"jean vautrin_bloody mary.fr.xml",
"TR08-7200tv8.conllu":"jean-marie le clézio_le procès-verbal.fr.xml",
"TR09-7200tv8.conllu":"jean-patrick manchette_morgue pleine.fr.xml",
"TR10-7200tv8.conllu":"jean-patrick manchette_trois hommes à abattre.fr.xml",
"TR11-7200tv8.conllu":"jean-paul sartre_les mots.fr.xml",
"TR12-7200tv8.conllu":"joseph joffo_un sac de billes.fr.xml",
"TR13-7200tv8.conllu":"jules romains_knock ou le triomphe de la médecine.fr.xml",
"TR14-7200tv8.conllu":"jules verne_le tour du monde en 80 jours.fr.xml",
"TR15-7200tv8.conllu":"julien green_léviathan.fr.xml",
"TR16-7200tv8.conllu":"léo malet_corrida aux champs-elysées.fr.xml",
"TR17-7200tv8.conllu":"léo malet_des kilomètres de linceuls.fr.xml",
"TR18-7200tv8.conllu":"léo malet_fièvre au marais.fr.xml",
"TR19-7200tv8.conllu":"léon-paul fargue_le piéton de paris.fr.xml",
"TR20-7200tv8.conllu":"louis-ferdinand céline_mort à crédit.fr.xml",
"TR21-7200tv8.conllu":"louis-ferdinand céline_rigodon.fr.xml",
"TR22-7200tv8.conllu":"louis-ferdinand céline_voyage au bout de la nuit.fr.xml",
"TR23-7200tv8.conllu":"marc lévy_et si c'était vrai-PREFAB.fr.x",
"TR39-7200tv8.conllu":"philippe djian_vers chez les blancs-PREFAB.fr.xml",
"TR24-7200tv8.conllu":"marcel aymé_la jument verte.fr.xml",
"TR25-7200tv8.conllu":"marcel pagnol_la gloire de mon père.fr.xml",
"TR26-7200tv8.conllu":"marcel pagnol_le temps des secrets. souvenirs d'enfance.fr.xml",
"TR27-7200tv8.conllu":"marguerite duras_hiroshima mon amour.fr.xml",
"TR28-7200tv8.conllu":"marguerite duras_les petits chevaux de tarquinia.fr.xml",
"TR29-7200tv8.conllu":"marguerite yourcenar_mémoires d'hadrien.fr.xml",
"TR30-7200tv8.conllu":"marie cardinal_la clé sur la porte.fr.xml",
"TR31-7200tv8.conllu":"maurice druon_les rois maudits. le roi de fer.fr.xml",
"TR32-7200tv8.conllu":"maurice renard_le docteur lerne sous-dieu.fr.xml",
"TR33-7200tv8.conllu":"maxence vandermeesch_corps et âmes.fr.xml",
"TR34-7200tv8.conllu":"nicole de buron_vas-y maman.fr.xml",
"TR35-7200tv8.conllu":"pascal lainé_la dentellière.fr.xml",
"TR36-7200tv8.conllu":"patrick cauvin_e = mc2 mon amour.fr.xml",
"TR37-7200tv8.conllu":"patrick modiano_rue des boutiques obscures.fr.xml",
"TR38-7200tv8.conllu":"paul nizon_stolz = thesaurus.fr.xml",
"TR40-7200tv8.conllu":"pierre magnan_le secret des andrônes.fr.xml",
"TR41-7200tv8.conllu":"pierre; narcejac thomas boileau_a coeur perdu meurtre en 45 tours.fr.xml",
"TR42-7200tv8.conllu":"pierre; narcejac thomas boileau_la lèpre.fr.xml",
"TR43-7200tv8.conllu":"pierre; narcejac thomas boileau_la porte du large.fr.xml",
"TR44-7200tv8.conllu":"prosper mérimée_colomba.fr.xml",
"TR45-7200tv8.conllu":"raymond aron_marxismes imaginaires. d'une sainte famille à l'autre.fr.xml",
"TR46-7200tv8.conllu":"raymond queneau_zazie dans le métro.fr.xml",
"TR47-7200tv8.conllu":"régine déforges_blanche et lucie.fr.xml",
"TR48-7200tv8.conllu":"robert brasillach_marchand d'oiseaux.fr.xml",
"TR49-7200tv8.conllu":"robert merle_la mort est mon métier.fr.xml",
"TR50-7200tv8.conllu":"roger caillois_des jeux et des hommes le masque et le vertige.fr.xml",
"TR51-7200tv8.conllu":"roger ikor_les eaux mêlées précédé de la greffe du printemps.fr.xml",
"TR52-7200tv8.conllu":"roger martin du gard_les thibault. la belle saison.fr.xml",
"TR53-7200tv8.conllu":"roger martin du gard_les thibault. le pénitencier.fr.xml",
"TR54-7200tv8.conllu":"romain gary_les racines du ciel..fr.xml",
"TR55-7200tv8.conllu":"sacha guitry_mémoires d'un tricheur.fr.xml",
"TR56-7200tv8.conllu":"sempé; goscinny_le petit nicolas.fr.xml",
"TR57-7200tv8.conllu":"serge & anne golon_angélique et le roi.fr.xml",
"TR58-7200tv8.conllu":"simone de beauvoir_l'âge de discrétion.fr.xml",
"TR59-7200tv8.conllu":"simone de beauvoir_la femme rompue.fr.xml",
"TR60-7200tv8.conllu":"simone de beauvoir_le sang des autres.fr.xml",
"TR61-7200tv8.conllu":"simone de beauvoir_mémoires d'une jeune fille rangée.fr.xml",
"TR62-7200tv8.conllu":"simone de beauvoir_monologue tiré de la femme rompue.fr.xml",
"TR63-7200tv8.conllu":"stendhal_la chartreuse de parme.fr.xml",
"TS01-7200tv8.conllu":"albert camus_l'étranger.fr.xml",
"TS02-7200tv8.conllu":"albert camus_la peste.fr.xml",
"TS03-7200tv8.conllu":"albertine sarrazin_l'astragale.fr.xml",
"TS04-7200tv8.conllu":"alphonse daudet_lettres de mon moulin.fr.xml",
"TS05-7200tv8.conllu":"andré françois-poncet_souvenirs d'une ambassade … berlin septembre 1931 - octobre 1938.fr.xml",
"TS06-7200tv8.conllu":"andré roussin_la voyante.fr.xml",
"TS07-7200tv8.conllu":"armand salacrou_les nuits de la colère.fr.xml",
"TS08-7200tv8.conllu":"arthur adamov_la politique des restes.fr.xml",
"TS09-7200tv8.conllu":"auguste de villiers de l'isle-adam_contes cruels la reine ysabeau.fr.xml",
"TS10-7200tv8.conllu":"boris vian_l'écume des jours.fr.xml",
"TS11-7200tv8.conllu":"choderlos de laclos_les liaisons dangereuses.fr.xml",
"TS12-7200tv8.conllu":"claude lévi-strauss_tristes tropiques.fr.xml",
"TS13-7200tv8.conllu":"edmond & jules de goncourt_madame de pompadour.fr.xml",
"TS14-7200tv8.conllu":"emile = romain gary ajar_la vie devant soi.fr.xml",
"TS15-7200tv8.conllu":"émile zola_germinal.fr.xml",
"TS16-7200tv8.conllu":"émile zola_les coquillages de m. chabre.fr.xml",
"TS17-7200tv8.conllu":"émile zola_nana.fr.xml",
"TS18-7200tv8.conllu":"ernest feydeau_fanny.fr.xml",
"TS19-7200tv8.conllu":"eugène ionesco_les chaises.fr.xml",
"TS20-7200tv8.conllu":"eugène ionesco_les rhinocéros.fr.xml",
"TS21-7200tv8.conllu":"françois cavanna_les russkoffs.fr.xml",
"TS22-7200tv8.conllu":"françoise dorin_l'autre valse.fr.xml",
"TS23-7200tv8.conllu":"françoise dorin_si t'es beau… t'es con.fr.xml",
"TS24-7200tv8.conllu":"françoise mallet-joris_la maison de papier.fr.xml",
"TS25-7200tv8.conllu":"frédérique hébrard_la vie reprendra au printemps.fr.xml",
"TS26-7200tv8.conllu":"gabriel chevallier_clochemerle.fr.xml",
"TS27-7200tv8.conllu":"george simenon_dimanche.fr.xml",
"TS28-7200tv8.conllu":"george simenon_en cas de malheur.fr.xml",
"TS29-7200tv8.conllu":"george simenon_félicie est là.fr.xml",
"TS30-7200tv8.conllu":"george simenon_l'ombre chinoise.fr.xml",
"TS37-7200tv8.conllu":"george simenon_les complices.fr.xml",
"TS38-7200tv8.conllu":"george simenon_maigret et l'inspecteur malgracieuxmaigret und der brummige inspektor.fr.xml",
"TS39-7200tv8.conllu":"george simenon_maigret et la vieille dame.fr.xml",
"TS40-7200tv8.conllu":"george simenon_maigret hésite.fr.xml",
"TS41-7200tv8.conllu":"george simenon_monsieur la souris.fr.xml",
"TS42-7200tv8.conllu":"georges courteline_boubouroche.fr.xml",
"TS43-7200tv8.conllu":"georges courteline_l'article 330.fr.xml",
"TS44-7200tv8.conllu":"georges courteline_la paix chez soi.fr.xml",
"TS45-7200tv8.conllu":"georges courteline_le commissaire est bon enfant.fr.xml",
"TS46-7200tv8.conllu":"georges courteline_le gendarme est sans pitié.fr.xml",
"TS47-7200tv8.conllu":"georges courteline_les balances.fr.xml",
"TS48-7200tv8.conllu":"georges duhamel_le notaire du havre.fr.xml",
"TS49-7200tv8.conllu":"georges duhamel_vue de la terre promise.fr.xml",
"TS50-7200tv8.conllu":"georges feydeau_le dindon.fr.xml",
"TS51-7200tv8.conllu":"georges friedmann_où va le travail humain.fr.xml",
"TS53-7200tv8.conllu":"gustave flaubert_madame bovary.fr.xml",
"TS54-7200tv8.conllu":"guy de maupassant_contes et nouvelles 1881 tôme 1. la maison tellier au printemps.fr.xml",
"TS55-7200tv8.conllu":"guy de maupassant_la bûche.fr.xml",
"TS56-7200tv8.conllu":"henry de montherlant_le démon du bien.fr.xml",
"TS57-7200tv8.conllu":"henry de montherlant_les jeunes filles.fr.xml",
"TS58-7200tv8.conllu":"henry de montherlant_les lépreuses.fr.xml",
"TS59-7200tv8.conllu":"henry murger_scènes de la vie de bohème.fr.xml",
"TS60-7200tv8.conllu":"hervé bazin_vipère au poing.fr.xml",
"TS61-7200tv8.conllu":"honoré de balzac_la femme de trente ans.fr.xml",
"TS62-7200tv8.conllu":"honoré de balzac_le colonel chabert.fr.xml",
"TS63-7200tv8.conllu":"honoré de balzac_splendeurs et misères des courtisanes.fr.xml",
"TS64-7200tv8.conllu":"jean anouilh_l'alouette.fr.xml",
"TS65-7200tv8.conllu":"jean cocteau_bacchus.fr.xml"
}
name_converterDE_FR = {
"DE01-7200tv8.conllu":"bernhard schlink_la fin de selb.fr.xml",
"DE02-7200tv8.conllu":"bettina violet_le sauvage enfant-lion.fr.xml",
"DE03-7200tv8.conllu":"birgit vanderbeke_devine ce que je vois.fr.xml",
"DE04-7200tv8.conllu":"bodo kirchhoff_infanta.fr.xml",
"DE05-7200tv8.conllu":"christa wolf_trame d'enfance.fr.xml",
"DE06-7200tv8.conllu":"claus-ulrich; grass günter; stolz dieter bielefeld_l'auteur et son agent secret. entretien.fr.xml",
"DE07-7200tv8.conllu":"elfriede jelinek_la pianiste.fr.xml",
"DE08-7200tv8.conllu":"elias canetti_comédie des vanités.fr.xml",
"DE09-7200tv8.conllu":"elke schmitter_madame sartoris.fr.xml",
"DE10-7200tv8.conllu":"erich-maria remarque_l'obélisque noir.fr.xml",
"DE11-7200tv8.conllu":"frank göhre_st.-pauli-nacht.fr.xml",
"DE12-7200tv8.conllu":"friedrich dürrenmatt_la promesse.fr.xml",
"DE13-7200tv8.conllu":"friedrich dürrenmatt_la visite de la vieille dame.fr.xml",
"DE14-7200tv8.conllu":"friedrich dürrenmatt_le juge et son bourreau.fr.xml",
"DE15-7200tv8.conllu":"friedrich dürrenmatt_les physiciens.fr.xml",
"DE16-7200tv8.conllu":"friedrich dürrenmatt_romulus le grand.fr.xml",
"DE17-7200tv8.conllu":"hans-peter; schumann harald martin_le piège de la mondialisation/ l'agression contre la démocratie et la prospérité.fr.xml",
"DE18-7200tv8.conllu":"heinrich böll_fin de mission.fr.xml",
"DE19-7200tv8.conllu":"heinrich böll_la grimace.fr.xml",
"DE20-7200tv8.conllu":"heinz g. konsalik_l'ange des oubliés.fr.xml",
"DE21-7200tv8.conllu":"helmut schmidt_la volonté de la paix.fr.xml",
"DE22-7200tv8.conllu":"josef roth_croquis de voyage.fr.xml",
"DE23-7200tv8.conllu":"karl valentin_le grand feu d'artifice et d'autres sketches.fr.xml",
"DE24-7200tv8.conllu":"manès sperber_plus profond que l'abîme.fr.xml",
"DE25-7200tv8.conllu":"marion gräfin von dönhoff_une enfance en prusse orientale.fr.xml",
"DE26-7200tv8.conllu":"marlen haushofer_le mur invisible.fr.xml",
"DE27-7200tv8.conllu":"martin r. dean_la ballade de billie et joe.fr.xml",
"DE28-7200tv8.conllu":"martin suter_la face cachée de la lune.fr.xml",
"DE29-7200tv8.conllu":"martin walser_chêne et lapin angora.fr.xml",
"DE30-7200tv8.conllu":"patrick süskind_le pigeon.fr.xml",
"DE31-7200tv8.conllu":"rolf schneider_le voyage à iaroslaw.fr.xml",
"DE32-7200tv8.conllu":"siegfried lenz_le bateau-phare.fr.xml",
"DE33-7200tv8.conllu":"thomas bernhard_le souffle - une décision.fr.xml",
"DE34-7200tv8.conllu":"thomas rosenlöcher_la meilleure façon de marcher. voyage dans le harz.fr.xml",
"DE35-7200tv8.conllu":"walter kempowski_les temps héroïques 3e partie.fr.xml"
}

# specify paths:
	#outputpath = path where output to be exported
	# conll_path : path to folder containing conllu annotations
	# xml_path : path to folder containing XML files with dd tags
outputpath = '/Users/Data/ANR_PREFAB/CorpusPREFAB/to_depparse/romans/PREFAB_XML_ROMANS_ALL/NUM/'
conll_path = '/Users/Data/ANR_PREFAB/CorpusPREFAB/to_depparse/romans/V3_7200s_all/NUM/'
xml_path = '/Users/Data/ANR_PREFAB/CorpusPREFAB/to_depparse/romans/FR_DE_xml_dd/'
log = []
# remember to specify sub-corpus in line 687 and converter dict to use in line 688 and log_name in line 747 !

# get conllu files and the basename of the folders to operate on
item_list = glob.glob(conll_path + "*.conllu")
item_list =[ item.replace(conll_path,'').replace("-7200tv8.conllu",'') for item in item_list]
# PO 37, 45, errror ; PO76 has no dates inside
for n in tqdm(range(67,100)):
 #if n != 22:
 #conll_file = '/Users/Data/ANR_PREFAB/CorpusPREFAB/Romans/processing_romans_tranches/sent_to_lexicoscope/PO/PO_archives_with_7000_with_sent_meta_no_linebreak/PO71/PO71-6022v2tv8.conllu'
 if n<10:
  n = "0"+str(n)
 filename = f'GE{n}-7200tv8.conllu'
 xml_name = name_converterGE.get(filename)
 XMLoutput_filename = outputpath + xml_name.replace(".xml", "-PREFABv2.xml")
 conll_file = conll_path + filename
 conll_data = import_prepare_for_stanza(conll_file)
 
 if os.path.exists(outputpath) is False:
  os.mkdir(outputpath)
 # specify file with XML dd tags, open, parse xmltree
 xml_file = xml_path + xml_name
 
 # parse the etree and get the root, s_tags and the children of the root = TEI.2 blocks
 tree = etree.parse(xml_file)  
 root = tree.getroot()
 s_tags = root.findall(".//s")
 contents = root.getchildren()
 
 #wordcount = get_wordcount(conll_data) ## also need to get new word count and overwrite value in input xml
 wordcount = conll_data.num_tokens
 words_number_element = root.find('.//wordsNumber')
 xml_words_init = words_number_element.get('value')
 # words_number_element.set('value', str(wordcount))
 
 # get sent counts to ensure a match
 xml_sents = len(s_tags)
 conll_sents = len(conll_data.sentences)
 
 # enumerate over s_tags, 
 for s, s_tag in enumerate(s_tags):
   # set tag_.text to joined LC of conll_text for token in sent.tokens, for each sentence. Thus the new annotations are inserted in place of the existing annotations
   s_tag.text = "\n" + "\n".join([token.to_conll_text() for token in conll_data.sentences[s].tokens]) + "\n"
 # determine which texts to keep and which not to print by using parent.remove(child_to_remove) :: this block isn't present in the special writer for DE-FR/FR-DE texts
 for blocl in contents:
  pub_date = blocl.find('.//pubDate').values()[0]
  if pub_date and int(pub_date) > 1979:
   print(f'Keeping {pub_date}')
   report_value = pub_date
  elif pub_date and int(pub_date) < 1979:
   parent = blocl.getparent()
   parent.remove(blocl)
   print(f'Dropping {pub_date}')
   report_value = pub_date
  else:
   print("no_pubdate")
   report_value = "no_pubdate"
 
 # export everything to xml
 tree.write(XMLoutput_filename, encoding = 'utf-8', xml_declaration=True)  
 # calculate % diff in length ; usually less than 5 %
 delta = 1-(int(wordcount)/int(xml_words_init))
 report = conll_file, XMLoutput_filename, xml_sents, conll_sents, xml_words_init, wordcount, delta, report_value 
 log.append(report)
 # temp_tree = etree.parse(tempfilepath)
 del(xml_file, tree)

# print log
log_df = pd.DataFrame(log)
log_df.to_csv('/Users/Data/GE_log.csv')





### special writer for NUM, TR, TS
outputpath = '/Users/Data/ANR_PREFAB/CorpusPREFAB/to_depparse/romans/PREFAB_XML_ROMANS_ALL/DFTRS/'
conll_path = '/Users/Data/ANR_PREFAB/CorpusPREFAB/to_depparse/romans/V3_7200s_all/DFTRS/'
xml_path = '/Users/Data/ANR_PREFAB/CorpusPREFAB/Romans/aligned_prefab_XML/XML_PREFAB_FR_de/'

#log = []
item_list = glob.glob(conll_path + "*.conllu")
item_list =[ item.replace(conll_path,'').replace("-7200tv8.conllu",'') for item in item_list]
item = item_list[-1]
for item in tqdm(item_list):
 filename = item +"-7200tv8.conllu"
 xml_name = name_converterDTRS.get(filename)
 xml_name = xml_name.replace(".fr.xml", "-PREFAB.fr.xml") # line for FR_DE aligned
 # xml_name = xml_name.replace(".fr.xml", ".fr-PREFAB.xml") # line for when have DE_FR aligned
 XMLoutput_filename = outputpath + xml_name.replace("PREFAB.fr.xml", "PREFABv2.fr.xml")
 conll_file = conll_path + filename
 conll_data = import_prepare_for_stanza(conll_file)

 if os.path.exists(outputpath) is False:
  os.mkdir(outputpath)
 # specify file with XML dd tags, open, parse xmltree
 xml_file = xml_path + xml_name
 if filename == "DE17-7200tv8.conllu":
  xml_file = "/Users/Data/ANR_PREFAB/CorpusPREFAB/Romans/aligned_prefab_XML/XML_PREFAB_de_FR/hans-peter; schumann harald martin_le piège de la mondialisation: l'agression contre la démocratie et la prospérité.fr-PREFAB.xml"
 tree = etree.parse(xml_file)  
 root = tree.getroot()
 s_tags = root.findall(".//s")
 contents = root.getchildren()
 
 #wordcount = get_wordcount(conll_data) ## also need to get new word count and overwrite value in input xml
 wordcount = conll_data.num_tokens
 words_number_element = root.find('.//wordsNumber')
 xml_words_init = words_number_element.get('value')
 # words_number_element.set('value', str(wordcount))
 
 # get sent counts to ensure a match
 xml_sents = len(s_tags)
 conll_sents = len(conll_data.sentences)
 
 # enumerate over s_tags, 
 for s, s_tag in enumerate(s_tags):
   # set tag_.text to joined LC of conll_text for token in sent.tokens, for each sentence. Thus the new annotations are inserted in place of the existing annotations
   s_tag.text = "\n" + "\n".join([token.to_conll_text() for token in conll_data.sentences[s].tokens]) + "\n"

 if os.path.exists(XMLoutput_filename) is True:
  XMLoutput_filename = XMLoutput_filename.replace('.xml',"from_" + filename + "_.xml")
 # export everything to xml
 tree.write(XMLoutput_filename, encoding = 'utf-8', xml_declaration=True)  
 
 delta = 1-(int(wordcount)/int(xml_words_init))
 report = conll_file, XMLoutput_filename, xml_sents, conll_sents, xml_words_init, wordcount, delta, report_value 
 log.append(report)
 # temp_tree = etree.parse(tempfilepath)
 del(xml_file, tree)


log_df = pd.DataFrame(log)
log_df.to_csv('/Users/Data/Aligned_log.csv')






######### after export, run this to make an inventory of what was exported and where for PhrasroRom files which can contain multiple ouvrages, but works on all XML output.


all_results, details = [], []
target_folder = '/Users/Data/ANR_PREFAB/CorpusPREFAB/to_depparse/romans/PREFAB_XML_ROMANS_ALL/NUM/'
xml_files = xml_files + glob.glob(target_folder+ '*.xml')
len(xml_files)
for xml_file in tqdm(xml_files):
   tree = etree.parse(xml_file)  
   root = tree.getroot()
   s_tags = root.findall(".//s")
   xmllen = (len(s_tags))
   words_number_element = root.findall('.//wordsNumber')
   xmlwordcount= [words_number_element[i].items() for i in range(len(words_number_element))]
   # words_number_init = words_number_element
   titles = root.findall('.//title')
   years = root.findall('.//pubDate')
   detail = [(xml_file.replace(target_folder, ''), title.values()[0], year.values()[0], xmlw_count[0][1]) for title, year, xmlw_count in zip(titles ,years, xmlwordcount)]
   details.append(detail)
   # words_number_element.set('value', str(wordcount))
  # enumerate over s_tags, 
# denest the list comprehension and export to xlsx
tidy_list = ([cell for well in details for cell in well])
detaildf = pd.DataFrame(tidy_list)
detaildf.columns = ['xml_file', 'title_ouvrage', 'year', 'words']
detaildf.to_excel("/Users/Data/Aligned_details.xlsx")
# print a sorted list of years to sure no false positives
sorted(detaildf.year.value_counts().index)




##function to insert dd_status from 6230 into 7300

file_with_ddstatus = '/Users/Data/ANR_PREFAB/CorpusPREFAB/Romans/RomansV3_d1/phraseorom_prefabV2/target/FY02-6230v2v2tv8.conllu'
file_without_dd_status = '/Users/Data/ANR_PREFAB/CorpusPREFAB/Romans/RomansV3_d1/phraseorom_prefabV2/processed/7300/FY02-7300v2v2tv8.conllu'
data_holder = []
dd_status_doc = CoNLL.conll2doc(file_with_ddstatus)
for sentence in tqdm(dd_status_doc.sentences):
  output = sentence.comments[-2], sentence.comments[-1]
  data_holder.append(output)
del(dd_status_doc)  

doc_without_ddststus = CoNLL.conll2doc(file_without_dd_status)
for t, sentence in enumerate(doc_without_ddststus.sentences):
  comment_toadd = data_holder[t][1]
  sentence.add_comment(comment_toadd)  
  sentence.comments
