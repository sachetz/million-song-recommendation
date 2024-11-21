import os
import pandas as pd
import h5py

def convert_to_df(file_path):
    h5 = h5py.File(file_path, 'r')
    store = pd.HDFStore(file_path, mode = 'r')

    song = pd.concat([
        store.get("metadata/songs"),
        store.get("analysis/songs"),
        store.get("musicbrainz/songs"),
        pd.Series([[item for item in list(h5.get("analysis/bars_confidence"))]], name = "bars_confidence"),
        pd.Series([[item for item in list(h5.get("analysis/bars_start"))]], name = "bars_start"),
        pd.Series([[item for item in list(h5.get("analysis/beats_confidence"))]], name = "beats_confidence"),
        pd.Series([[item for item in list(h5.get("analysis/beats_start"))]], name = "beats_start"),
        pd.Series([[item for item in list(h5.get("analysis/sections_confidence"))]], name = "sections_confidence"),
        pd.Series([[item for item in list(h5.get("analysis/sections_start"))]], name = "sections_start"),
        pd.Series([[item for item in list(h5.get("analysis/segments_confidence"))]], name = "segments_confidence"),
        pd.Series([[item for item in list(h5.get("analysis/segments_loudness_max"))]], name = "segments_loudness_max"),
        pd.Series([[item for item in list(h5.get("analysis/segments_loudness_max_time"))]], name = "segments_loudness_max_time"),
        pd.Series([[item for item in list(h5.get("analysis/segments_loudness_start"))]], name = "segments_loudness_start"),
        pd.Series([[item for item in list(h5.get("analysis/segments_pitches"))]], name = "segments_pitches"),
        pd.Series([[item for item in list(h5.get("analysis/segments_start"))]], name = "segments_start"),
        pd.Series([[item for item in list(h5.get("analysis/segments_timbre"))]], name = "segments_timbre"),
        pd.Series([[item for item in list(h5.get("analysis/tatums_confidence"))]], name = "tatums_confidence"),
        pd.Series([[item for item in list(h5.get("analysis/tatums_start"))]], name = "tatums_start"),
        pd.Series([[item.decode() for item in list(h5.get("metadata/artist_terms"))]], name = "artist_terms"),
        pd.Series([[item for item in list(h5.get("metadata/artist_terms_freq"))]], name = "artist_terms_freq"),
        pd.Series([[item for item in list(h5.get("metadata/artist_terms_weight"))]], name = "artist_terms_weight"),
        pd.Series([[item.decode() for item in list(h5.get("metadata/similar_artists"))]], name = "similar_artists"),
        pd.Series([[item for item in list(h5.get("musicbrainz/artist_mbtags"))]], name = "artist_mbtags"),
        pd.Series([[item for item in list(h5.get("musicbrainz/artist_mbtags_count"))]], name = "artist_mbtags_count")
    ], axis=1, join='outer')

    columns_to_use = [
        "analysis_sample_rate",
        "artist_7digitalid",
        "artist_familiarity",
        "artist_hotttnesss",
        "artist_id",
        "artist_latitude",
        "artist_location",
        "artist_longitude",
        "artist_mbid",
        "artist_mbtags",
        "artist_mbtags_count",
        "artist_name",
        "artist_playmeid",
        "artist_terms",
        "artist_terms_freq",
        "artist_terms_weight",
        "audio_md5",
        "bars_confidence",
        "bars_start",
        "beats_confidence",
        "beats_start",
        "danceability",
        "duration",
        "end_of_fade_in",
        "energy",
        "key",
        "key_confidence",
        "loudness",
        "mode",
        "mode_confidence",
        "release",
        "release_7digitalid",
        "sections_confidence",
        "sections_start",
        "segments_confidence",
        "segments_loudness_max",
        "segments_loudness_max_time",
        "segments_loudness_start",
        "segments_pitches",
        "segments_start",
        "segments_timbre",
        "similar_artists",
        "song_hotttnesss",
        "song_id",
        "start_of_fade_out",
        "tatums_confidence",
        "tatums_start",
        "tempo",
        "time_signature",
        "time_signature_confidence",
        "title",
        "track_id",
        "track_7digitalid",
        "year"
    ]
    song = song[columns_to_use]

    store.close()
    h5.close()
    
    return song

def process_directory(dir_path, output_file):
    dfs = []

    for root, dirs, files in os.walk(dir_path):
        for file in files:
            if file.endswith(".h5"):
                file_path = os.path.join(root, file)
                try:
                    df = convert_to_df(file_path)
                    dfs.append(df)
                except Exception as e:
                    print(f"Error processing {file_path}: {e}")

    # Combine all data into a single DataFrame
    combined_df = pd.concat(dfs, ignore_index=True)
    # Write to parquet
    combined_df.to_parquet(output_file, engine="pyarrow")


process_directory("/Users/sachetz/uchicago_mpcs/big_data_app_arch/A/A/A", "A.parquet")
