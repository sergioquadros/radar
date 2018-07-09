# diretories: RMSP(with 38 746 files), Saida_Pluv(with 108 files) at workdir one
# Files at workdir diretory: 
# cloud_script.R; navegacao_rmsp.dat; coordenadas_pluv_saisp2.dat
library(knitr); library(rmarkdown);library(tidyverse)
library(gridExtra); library(corrplot); library(magrittr)
library(lubridate); library(parallel)
# numbers of cores --> initialize clusters 
no_cores <- detectCores()-1
cl <- makeCluster(no_cores)

# dim(mask_lon)[1]*dim(mask_lon)[2]=nx*ny=25776 ptos de grade
ny <- 179
nx <- 144
navegacao <- read.table("navegacao_rmsp.dat")  
navegacao[,1:2] <- navegacao[,1:2]+1 
# masks for navigation
mask_lon <- array(dim = c(nx, ny))
mask_lat <- array(dim = c(nx, ny))
for(m in 1:25776){
  i <- navegacao[m,1]
  j <- navegacao[m,2]
  mask_lon[i,j] <- navegacao[m,3] # recebe lon
  mask_lat[i,j] <- navegacao[m,4] # recebe lat
}
caminho <- list.files(path=paste0(getwd(), "/RMSP"), full.names = TRUE)
dia <- list.files(path=paste0(getwd(), "/RMSP"))
radarF <- cbind.data.frame(caminho, dia)
radarF$dia %<>% gsub(pattern = ".bin", replacement = "") 
radarF$caminho %<>% as.character
caminho <- dia <- NULL

# id lat lon de 108 pluviometers
pluviometro <- read.csv(file="coordenadas_pluv_saisp2.dat") 
# add full path for and (i,j) from masks
pluviometro %<>% mutate(linha=0.0, coluna=0.0, 
                        arquivo=paste0(getwd(),"/Saida_Pluv/", id, ".dat")) 
for (p in 1:108) {
  onde_min <- which.min((navegacao$V4-pluviometro$lat[p])^2+(navegacao$V3-pluviometro$lon[p])^2)
  pluviometro$linha[p] <- navegacao[onde_min,1]
  pluviometro$coluna[p] <- navegacao[onde_min,2]
}
pluviometro$id %<>% as.character
pluviometro$id %<>% as.factor

# the faz_nove function copies nine values around the given coordinate
faz_nove <- function(chuva, i, j, nx, ny){
  Aux <- rep_len(0.0,9)
  Aux[5] <- chuva[i,j]
  if(j-1==0 | j+1>ny){
    Aux[4] <- NA
    Aux[6] <- NA
  }else{
    Aux[4] <- chuva[i,j-1]
    Aux[2] <- chuva[i,j+1]
  }
  if(i-1==0){
    Aux[1] <- NA
    Aux[2] <- NA
    Aux[3] <- NA
  }else{
    Aux[1] <- chuva[i-1,j-1]
    Aux[2] <- chuva[i-1,j]
    Aux[3] <- chuva[i-1,j+1]
  }
  if(i+1>nx){
    Aux[7] <- NA
    Aux[8] <- NA
    Aux[9] <- NA
  }else{
    Aux[7] <- chuva[i+1,j-1]
    Aux[8] <- chuva[i+1,j]
    Aux[9] <- chuva[i+1,j+1]
  }
  return(Aux)
}

plu <- 1
pingo <- read.table(file(pluviometro$arquivo[plu]), header = FALSE, 
                    colClasses = c("character", "character", "character",
                                   "character", "character", "numeric"))
colnames(pingo) <- c("ano", "mes", "dia", "hora", "minuto", "Rpluv")
pingo %<>% mutate(id = as.factor(pluviometro$id[plu]), 
                  linha = pluviometro$linha[plu], 
                  coluna = pluviometro$coluna[plu], 
                  tempo = paste0(ano,mes,dia,hora,minuto))
pingo <- pingo[,-c(1:5)]
for(plu in 2:108){
  aux <- read.table(file(pluviometro$arquivo[plu]), header = FALSE, 
                    colClasses = c("character", "character", "character",
                                   "character", "character", "numeric"))
  colnames(aux) <- c("ano", "mes", "dia", "hora", "minuto", "Rpluv")
  aux %<>% mutate(id = as.factor(pluviometro$id[plu]), 
                  linha = pluviometro$linha[plu], 
                  coluna = pluviometro$coluna[plu], 
                  tempo = paste0(ano,mes,dia,hora,minuto))
  aux <- aux[,-c(1:5)]
  pingo %<>% rbind.data.frame(aux)
}
aux <- NULL
# Adição de variáveis Rradar(média) e sd_Rradar(desvio-padrão) à df "pingo"
pingo %<>% mutate(Rradar = 0.0, sd_Rradar = 0.0)

# two loop
# clusterExport: pluviometro, nx, ny, pingo, faz_nove
clusterExport(cl,c("faz_nove","pingo","nx", "ny", "pluviometro")) 
# the first loop
for(obs in 1:38746){
  radar <- file(radarF$caminho[obs], "rb")
  bindata <- readBin(radar, numeric(), size=4, n=25776)
  close(radar)
  hora <- radarF$dia[obs]
  chuva <- matrix(data = bindata, nrow = nx, ncol = ny)
  # for each loop a modified "hora" variable and "chuva" matrix
  clusterExport(cl,c("hora","chuva"))
  # the second loop is more time consuming
  plu <- 1:108
  parSapply(cl, plu, function(plu){
    i <- pluviometro$linha[plu]
    j <- pluviometro$coluna[plu]
    xx <- faz_nove(chuva, i, j, nx, ny)
    este <- which(pingo$tempo==hora & pingo$id==pluviometro$id[plu])
    pingo$Rradar[este] <- mean(xx, na.rm = TRUE)
    pingo$sd_Rradar[este] <- sd(xx, na.rm = TRUE)
  })
}
chuva <- bindata <- NULL

# the desired file
write.table(pingo, "output.csv", sep = ",", col.names = T)

# end of cluster
stopCluster(cl)
