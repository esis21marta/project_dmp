from sqlalchemy import ARRAY, Column, DateTime, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class FileRecord(Base):
    __tablename__ = "aggregator_job_files"

    id = Column(Integer, primary_key=True)
    file_name = Column(String)
    file_path = Column(String)
    status = Column(String)
    engine = Column(String)
    run_id = Column(Integer)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    temp_keyword_ids = Column(ARRAY(Integer))

    def __repr__(self):
        return f"<FileRecord(id={self.id}, \
            file_name={self.file_name}, \
            file_path={self.file_path}, \
            status={self.status}, \
            engine={self.engine}, \
            run_id={self.run_id}, \
            created_at={self.created_at}, \
            updated_at={self.updated_at}, \
            temp_keyword_ids={self.temp_keyword_ids})>"


class Run(Base):
    __tablename__ = "aggregator_runs"

    run_id = Column(Integer, primary_key=True)
    status = Column(String)
    engine = Column(String)
    started_at = Column(DateTime)
    ended_at = Column(DateTime)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    top_rank = Column(Integer)
    channel_limits = Column(String)

    def __repr__(self):
        return f"<Run(run_id={self.run_id}, status={self.status}, \
        engine={self.engine}, started_at={self.started_at}, ended_at={self.ended_at}, \
            created_at={self.created_at}, updated_at={self.updated_at}, \
            top_rank={self.top_rank}, channel_limits={self.channel_limits})>"


class WhitelistFileRecord(Base):
    __tablename__ = "aggregator_whitelist_files"

    whitelist_id = Column(Integer, primary_key=True)
    whitelist_filepath = Column(String)
    run_id = Column(Integer)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)

    def __repr__(self):
        return f"<WhitelistFileRecord(whitelist_id={self.whitelist_id}, \
        whitelist_filepath={self.whitelist_filepath}, \
        run_id={self.run_id}, created_at={self.created_at}, updated_at={self.updated_at})>"


class CampaignCriteria(Base):
    __tablename__ = "cm_prioritization_criteria"

    p_criteria_id = Column(Integer, primary_key=True)
    p_rank_id = Column(Integer)
    created_at = Column(DateTime)
    deactivated_at = Column(DateTime)
    status = Column(String)
    criteria_rank = Column(String)
    criteria_desc = Column(String)

    def __repr__(self):
        return f"<CampCriteria(p_criteria_id={self.p_criteria_id}, \
            p_rank_id={self.p_rank_id}, \
            created_at={self.created_at}, \
            deactivated_at={self.deactivated_at}, \
            status={self.status}, \
            criteria_rank={self.criteria_rank}, \
            criteria_desc={self.criteria_desc})>"


class CampaignRank(Base):
    __tablename__ = "cm_prioritization_rank"

    p_rank_id = Column(Integer, primary_key=True)
    p_rank_batch_id = Column(Integer)
    created_at = Column(DateTime)
    deactivated_at = Column(DateTime)
    status = Column(String)
    campaign_family = Column(String)
    campaign_rank = Column(String)

    def __repr__(self):
        return f"<CampRank(p_rank_id={self.p_rank_id}, \
            p_rank_batch_id={self.p_rank_batch_id}, \
            created_at={self.created_at}, \
            deactivated_at={self.deactivated_at}, \
            status={self.status}, \
            campaign_family={self.campaign_family}, \
            campaign_rank={self.campaign_rank})>"


class CampaignConfig(Base):
    __tablename__ = "cm_campaign_config"

    config_id = Column(Integer, primary_key=True)
    created_at = Column(DateTime)
    deactivated_at = Column(DateTime)
    status = Column(String)
    campaign_family = Column(String)
    campaign_name = Column(String)
    push_channel = Column(String)
    dest_sys = Column(String)

    def __repr__(self):
        return f"<CampConfig(config_id={self.config_id}, \
            created_at={self.created_at}, \
            deactivated_at={self.deactivated_at}, \
            status={self.status}, \
            campaign_family={self.campaign_family}, \
            campaign_name={self.campaign_name}, \
            push_channel={self.push_channel}, \
            dest_sys={self.dest_sys})>"


class CampaignKeyword(Base):
    __tablename__ = "cm_generated_keyword"

    temp_keyword_id = Column(Integer, primary_key=True)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    temp_keyword = Column(String)
    confirmed_keyword = Column(String)
    config_id = Column(Integer)
    p_rank_id = Column(Integer)
    p_rank_batch_id = Column(Integer)
    agg_job_id = Column(Integer)

    def __repr__(self):
        return f"<CampKeyword(temp_keyword_id={self.temp_keyword_id}, \
            created_at={self.created_at}, \
            updated_at={self.updated_at}, \
            temp_keyword={self.temp_keyword}, \
            confirmed_keyword={self.confirmed_keyword}, \
            config_id={self.config_id}, \
            p_rank_id={self.p_rank_id}, \
            p_rank_batch_id={self.p_rank_batch_id}, \
            agg_job_id={self.agg_job_id})>"
