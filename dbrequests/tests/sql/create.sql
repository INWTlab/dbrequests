CREATE TABLE cats
          (
            id              INT unsigned NOT NULL AUTO_INCREMENT, # Unique ID for the record
            name            VARCHAR(150) NOT NULL,                # Name of the cat
            owner           VARCHAR(150) NOT NULL,                # Owner of the cat
            birth           DATE NOT NULL,                        # Birthday of the cat
            PRIMARY KEY     (id)                                  # Make the id the primary key
          );
