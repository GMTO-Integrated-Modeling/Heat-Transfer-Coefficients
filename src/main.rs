use parse_monitors::{MonitorsLoader,cfd::{CfdCase,Baseline,BaselineTrait}};
use polars::prelude::*;

fn main() -> anyhow::Result<()> {
    //Get one example cfd case (this line of code is no used in final version):
    let _cfdcase: CfdCase<2021> = CfdCase::colloquial(30,0,"os",7)?;

    // Setting the path to the directory with the data file: "monitors.csv.z"
    let path_to_data = Baseline::<2021>::default_path();

    //Create an empty polars DataFrame to append values to in for loop below:
    let mut htc_df = DataFrame::default();

    //Loop over all 60 cfd cases, take(2) to just work with the first two cases. 
    for cfdcase in Baseline::<2021>::default().into_iter().take(60){
        //Print the cfdcase:
        println!("{}", cfdcase.to_pretty_string());

        // Loading the "monitors" file
        let monitors = MonitorsLoader::<2021>::default()
            .data_path(path_to_data.join(cfdcase.to_string()))
            .load()?;
         
        // The HTC are available in the "heat_transfer_coefficients" property
        // println!(
        //     "HTC # of elements: {}",
        //    monitors.heat_transfer_coefficients.len()
        //);

        // For statiscal analysis, it may be a better option to import the data in a polars (https://pola-rs.github.io/polars-book/user-guide/index.html) dataframe
        let htc: DataFrame = monitors
            .heat_transfer_coefficients
            .into_iter()
            .map(|(key, value)| Series::new(&key, value))
            .collect();
        //println!("{}", htc.head(None));

        // select the chosen elements that represent the heat transfer coefficientes of interest:
        let htc2 = htc_select_element(&htc)?;
        //println!("{}", htc2);

        // calculate the mean of each heat transfer coefficient of interest:
        let htc3: DataFrame = {
            let df = &htc2;
            df.mean()
        };
        //println!("{:?}", htc3);

        //Stack the cfdcase information as additional columns: 
        let s1: Series = Series::new("Azimuth", &[cfdcase.azimuth.to_string()]);
        let s2: Series = Series::new("Zenith", &[cfdcase.zenith.to_string()]);
        let s3: Series = Series::new("Enclosure", &[cfdcase.enclosure.to_string()]);
        let s4: Series = Series::new("Windspeed", &[cfdcase.wind_speed.to_string()]);
        let htc4: PolarsResult<DataFrame> = htc3.hstack(&[s1, s2, s3, s4]);
        //println!("{:?}", htc4);

        //join successive dataframes:
        htc_df = htc_df.vstack(&htc4?).unwrap();

        }
    //Print out the final Polars DataFrame:
    //println!("{:?}", htc_df);

    //Save Polars DataFrame to a .csv file:
    let mut file = std::fs::File::create("HTC_2021_17elements_60cases.csv").unwrap();
    CsvWriter::new(&mut file).finish(&mut htc_df).unwrap();

    Ok(())
}

fn htc_select_element(df: &DataFrame) -> PolarsResult<DataFrame> {
    df.select(["truss", "Cring", "M1cell", 
    "topend",  "toprest", "CableTruss", "M1baffle", 
    "M1covers", "M1Level", "M2baffle", "M2seg", 
    "platforms", "floor", "GIR", "LGSS", "M1off", 
    "M1on"])
}
