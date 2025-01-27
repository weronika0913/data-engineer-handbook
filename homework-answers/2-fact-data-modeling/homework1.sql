-- Create a temporary dataset named `deduped` to remove duplicates based on game_id, team_id, and player_id
WITH deduped AS (
    SELECT 
        g.game_date_est, -- Game date from the `games` table
        gd.*, -- All columns from the `game_details` table
        ROW_NUMBER() OVER (
            PARTITION BY gd.game_id, gd.team_id, gd.player_id 
            ORDER BY g.game_date_est
        ) AS row_num -- Assign a unique row number within each group (game_id, team_id, player_id)
        -- ROW_NUMBER ensures we can select the first occurrence in the group based on the order of `game_date_est`
    FROM game_details AS gd
    JOIN games AS g 
        ON gd.game_id = g.game_id -- Join `game_details` with `games` on the game_id
)

-- Retrieve only the rows where `row_num = 1`, i.e., the first record in each group
SELECT *
FROM deduped
WHERE row_num = 1;
