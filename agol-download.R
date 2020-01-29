# Read tabular data from AGOL

## Get a token with a headless account
token_resp <- POST("https://nps.maps.arcgis.com/sharing/rest/generateToken",
                   body = list(username = rstudioapi::showPrompt("Username", "Please enter your AGOL username", default = "mojn_hydro"),
                               password = rstudioapi::askForPassword("Please enter your AGOL password"),
                               referer = 'https://irma.nps.gov',
                               f = 'json'),
                   encode = "form")
agol_token <- fromJSON(content(token_resp, type="text", encoding = "UTF-8"))

service_url = "https://services1.arcgis.com/fBc8EJBxQRMcHlei/arcgis/rest/services/service_92a9095970814482a0534c08a0628f38/FeatureServer"
# old_service_url = "https://services1.arcgis.com/fBc8EJBxQRMcHlei/arcgis/rest/services/service_91ba537840c94230a0bdfb2e96385070/FeatureServer"

## Get annual lake visit data
resp.visit <- GET(paste0(service_url, "/0/query"),
                  query = list(where="1=1",
                               outFields="*",
                               f="JSON",
                               token=agol_token$token))
visit <- fromJSON(content(resp.visit, type = "text", encoding = "UTF-8"))
visit <- visit$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999) %>%
  mutate(StartTime = as.POSIXct(StartTime/1000, origin = "1970-01-01", tz = "America/Los_Angeles")) %>%
  rename(StartDateTime = StartTime)

resp.dl <- GET(paste0(service_url, "/1/query"),
               query = list(where="1=1",
                            outFields="*",
                            f="JSON",
                            token=agol_token$token))
sensor.dl <- fromJSON(content(resp.dl, type = "text", encoding = "UTF-8"))
sensor.dl <- cbind(sensor.dl$features$attributes, sensor.dl$features$geometry) %>%
  mutate(wkid = sensor.dl$spatialReference$wkid) %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)

resp.deploy <- GET(paste0(service_url, "/2/query"),
                   query = list(where="1=1",
                                outFields="*",
                                f="JSON",
                                token=agol_token$token))
sensor.deploy <- fromJSON(content(resp.deploy, type = "text", encoding = "UTF-8"))
sensor.deploy <- cbind(sensor.deploy$features$attributes, sensor.deploy$features$geometry) %>%
  mutate(wkid = sensor.deploy$spatialReference$wkid) %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)

resp.photos <- GET(paste0(service_url, "/3/query"),
                   query = list(where="1=1",
                                outFields="*",
                                f="JSON",
                                token=agol_token$token))
photos <- fromJSON(content(resp.photos, type = "text", encoding = "UTF-8"))
photos <- cbind(photos$features$attributes, photos$features$geometry) %>%
  mutate(wkid = photos$spatialReference$wkid) %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)

resp.crew <- GET(paste0(service_url, "/4/query"),
                 query = list(where="1=1",
                              outFields="*",
                              f="JSON",
                              token=agol_token$token))
crew <- fromJSON(content(resp.crew, type = "text", encoding = "UTF-8"))
crew <- crew$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)

resp.wq <- GET(paste0(service_url, "/5/query"),
               query = list(where="1=1",
                            outFields="*",
                            f="JSON",
                            token=agol_token$token))
wq <- fromJSON(content(resp.wq, type = "text", encoding = "UTF-8"))
wq <- wq$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)

resp.secchi <- GET(paste0(service_url, "/6/query"),
                   query = list(where="1=1",
                                outFields="*",
                                f="JSON",
                                token=agol_token$token))
secchi <- fromJSON(content(resp.secchi, type = "text", encoding = "UTF-8"))
secchi <- secchi$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)

# resp.sample <- GET("https://services1.arcgis.com/fBc8EJBxQRMcHlei/arcgis/rest/services/service_91ba537840c94230a0bdfb2e96385070/FeatureServer/7/query",
#                    query = list(where="1=1",
#                                 outFields="*",
#                                 f="JSON",
#                                 token=agol_token$token))
# sample <- fromJSON(content(resp.sample, type = "text", encoding = "UTF-8"))
# sample <- sample$features$attributes %>%
#   as_tibble() %>%
#   mutate_if(is_character, na_if, "") %>%
#   mutate_if(is.numeric, na_if, -9999)

## Get lake levels data
levels_service_url = "https://services1.arcgis.com/fBc8EJBxQRMcHlei/arcgis/rest/services/service_e2571ff8454a4c65900a22297d10841f/FeatureServer"
resp.levels <- GET(paste0(service_url, "/0/query"),
                   query = list(where="1=1",
                                outFields="*",
                                f="JSON",
                                token=agol_token$token))
levels <- fromJSON(content(resp.levels, type = "text", encoding = "UTF-8"))
levels <- levels$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999) %>%
  mutate(StartTime = as.POSIXct(StartTime/1000, origin = "1970-01-01", tz = "America/Los_Angeles")) %>%
  rename(StartDateTime = StartTime)

resp.levels.crew <- GET(paste0(service_url, "/1/query"),
                        query = list(where="1=1",
                                     outFields="*",
                                     f="JSON",
                                     token=agol_token$token))
levels.crew <- fromJSON(content(resp.levels.crew, type = "text", encoding = "UTF-8"))
levels.crew <- levels.crew$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)

resp.benchphoto <- GET(paste0(service_url, "/2/query"),
                       query = list(where="1=1",
                                    outFields="*",
                                    f="JSON",
                                    token=agol_token$token))
bench.photo <- fromJSON(content(resp.benchphoto, type = "text", encoding = "UTF-8"))
bench.photo <- bench.photo$features$attributes %>%
  as_tibble() %>%
  mutate_if(is_character, na_if, "") %>%
  mutate_if(is.numeric, na_if, -9999)
