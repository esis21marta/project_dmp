CREATE TABLE cm_prioritization_rank (
    p_rank_id SERIAL PRIMARY KEY NOT NULL,
    p_rank_batch_id INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deactivated_at TIMESTAMP,
    status VARCHAR NOT NULL,
    campaign_family VARCHAR NOT NULL,
    campaign_rank INT NOT NULL);
    
CREATE TABLE cm_prioritization_criteria (
    p_criteria_id SERIAL PRIMARY KEY NOT NULL,
    p_rank_id INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deactivated_at TIMESTAMP,
    status VARCHAR NOT NULL,
    criteria_rank INT NOT NULL,
    criteria_desc VARCHAR);
    
CREATE TABLE cm_campaign_config (
    config_id SERIAL PRIMARY KEY NOT NULL,
    created_at TIMESTAMP NOT NULL,
    deactivated_at TIMESTAMP,
    status VARCHAR NOT NULL,
    campaign_family VARCHAR NOT NULL,
    campaign_name VARCHAR NOT NULL,
    push_channel VARCHAR NOT NULL,
    dest_sys VARCHAR NOT NULL);
    
    
CREATE TABLE IF NOT EXISTS cm_generated_keyword (
    temp_keyword_id SERIAL PRIMARY KEY NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    temp_keyword VARCHAR NOT NULL,
    confirmed_keyword VARCHAR,
    config_id INT NOT NULL,
    p_rank_id INT NOT NULL,
    p_rank_batch_id INT NOT NULL,
    agg_job_id INT);