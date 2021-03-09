import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AppImmo extends App {
  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("App Immobilier 2019")
    .getOrCreate()
  spark.sparkContext.setLogLevel("Error")

  println("### *** Fichier immobilier dans Toute la France Années 2014-2016-2017-2018-2019-2020 en format CSV *** ###")
  val immoDF = spark
    .read
    .option("header", "true")
    .format("csv")
    .csv("C:\\Users\\Nabil.BLIDI\\IdeaProjects\\Projet_Final_Immo\\src\\main\\Ressources\\Data_Immo_CSV")

  immoDF.show


  println("****************************************************************************************************************************")
  println("  ************************************ Fichier Immo ===> Maisons && Appartements ****************************************** ")
  println("### *** Fichier immobilier uniquement en Ile de France Années 2014-2016-2017-2018-2019-2020 *** ###")
  val selAppMaisonDF = immoDF
    .filter(
      "code_departement in (75,77,78,91,92,93,94,95) AND type_local != 'Local industriel. commercial ou assimilé' AND surface_reelle_bati >0")
    .select(
      "date_mutation",
      "valeur_fonciere",
      "nom_commune",
      "type_local",
      "surface_reelle_bati",
      "code_postal",
      "code_departement"
    )
  selAppMaisonDF.show(false)
  println(" Save Immo_Ile_De_France_CSV file local folder :save_res_10_Mars/Maisons_Appartements ")
  selAppMaisonDF
    .repartition(1)
    .write.format("com.databricks.spark.csv")
    .option("header", "true")
    .save("file:\\Users\\Nabil.BLIDI\\IdeaProjects\\Projet_Final_Immo\\src\\main\\Ressources\\Save_res_10_Mars\\Maisons_Appartements")


  println("***************************************************************************************************************")
  println("  ************************************ Fichier Immo Ile de France uniquement pour les  Maisons  ****************************************** ")
  println("### ********************************* Années 2014-2016-2017-2018-2019-2020     ******************************************")
  val selMaisonDF = immoDF
    .filter("code_departement in (75,77,78,91,92,93,94,95) AND type_local = 'Maison' AND surface_reelle_bati >0")
    .select(
      "date_mutation",
      "valeur_fonciere",
      "nom_commune",
      "type_local",
      "surface_reelle_bati",
      "code_postal",
      "code_departement"
    )
  selMaisonDF.show(false)
  println(" Save Immo_Ile_De_France_CSV file local folder :save_res_10_Mars/Maisons ")
  selMaisonDF
    .repartition(1)
    .write.format("com.databricks.spark.csv")
    .option("header", "true")
    .save("file:\\Users\\Nabil.BLIDI\\IdeaProjects\\Projet_Final_Immo\\src\\main\\Ressources\\Save_res_10_Mars\\Maisons")


  println("***************************************************************************************************************")
  println("  ************************************ Fichier Immo Ile de France uniquement pour les  appartements ****************************************** ")
  println("### ********************************* Années 2014-2016-2017-2018-2019-2020     ******************************************")
  val selApparDF = immoDF
    .filter("code_postal in (75008) AND type_local = 'Appartement' AND surface_reelle_bati >0")
    .select(
      "date_mutation",
      "valeur_fonciere",
      "nom_commune",
      "type_local",
      "surface_reelle_bati",
      "code_postal",
      "code_departement"
    )
  selApparDF.show(false)

  println(" Save Immo_Ile_De_France_CSV file local folder :save_res_10_Mars/Appartements ")
  selApparDF
    .repartition(1)
    .write.format("com.databricks.spark.csv")
    .option("header", "true")
    .save("file:\\Users\\Nabil.BLIDI\\IdeaProjects\\Projet_Final_Immo\\src\\main\\Ressources\\Save_res_10_Mars\\Appartements")


  println("*********************************************************************************************************************************")
  println("** Fichier JSON donnees-hospitalières-relatives-a-l'epidemie-de-covid-19-en-Ile-De-France ************************************************")
  println(" *** Read multiline Json file  ***")
  val DFJson = spark
    .read
    .format("json")
    .option("header", "true")
    .option("interSchema", "true")
    .option("multiline", "true")
    .json("C:\\Users\\Nabil.BLIDI\\IdeaProjects\\Projet_Final_Immo\\src\\main\\Ressources\\Data_Immo_Json")


  println("  ### Données-hospitalières-relatives-a-l'épidemie-de-covid-19-en-ile-de-france ###")
  DFJson.printSchema()
  DFJson.show(false)
  val selJsonDF = DFJson
    .filter("fields.sex !='Tous'")
    .select(
      "fields.date",
      "fields.sex",
      "fields.tot_death",
      "fields.dep_code"

    )

  selJsonDF.show()

  selJsonDF
    .repartition(1)
    .write.format("com.databricks.spark.csv")
    .option("header", "true")
    .save("file:\\Users\\Nabil.BLIDI\\IdeaProjects\\Projet_Final_Immo\\src\\main\\Ressources\\Save_res_10_Mars\\Covid-19_CSV")


  println("***************************************************************************************")
  println("************ Jointure entre Data Immo & Covid ==> Immo Maisons Appartements && Covid-19")
  val jointureMaisonAppDF = selAppMaisonDF
    .join(selJsonDF,
      selAppMaisonDF("code_departement") === selJsonDF("dep_code"),
      "inner")
    .drop(selJsonDF("dep_code"))

  jointureMaisonAppDF.show

  jointureMaisonAppDF
    .repartition(1)
    .write
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .save("file:\\Users\\Nabil.BLIDI\\IdeaProjects\\Projet_Final_Immo\\src\\main\\Ressources\\Output_Immo_Covid\\Jointure_Immo_Maison_App_Covid_CSV")


  println("******************************************************************************")
  println("************ Jointure entre Data Immo & Covid ==> Immo Appartements && Covid-19")
  val jointureAppDF = selApparDF
    .join(selJsonDF,
      selApparDF("code_departement") === selJsonDF("dep_code"),
      "inner")
    .drop(selJsonDF("dep_code"))

  jointureAppDF.show

  jointureAppDF
    .repartition(1)
    .write
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .save("file:\\Users\\Nabil.BLIDI\\IdeaProjects\\Projet_Final_Immo\\src\\main\\Ressources\\Output_Immo_Covid\\Jointure_Immo_App_Covid_CSV")


  println("*******************************************************************************************")
  println("************ Jointure entre Data Immo & Covid ==> Immo Maisons && Covid-19")
  val jointureMaisonDF = selMaisonDF
    .join(selJsonDF,
      selMaisonDF("code_departement") === selJsonDF("dep_code"),
      "inner")
    .drop(selJsonDF("dep_code"))

  jointureMaisonDF.show

  jointureMaisonDF
    .repartition(1)
    .write
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .save("file:\\Users\\Nabil.BLIDI\\IdeaProjects\\Projet_Final_Immo\\src\\main\\Ressources\\Output_Immo_Covid\\Jointure_Immo_Maison_Covid_CSV")

}
