from datetime import datetime
from pydantic import BaseModel


class RawSoccerMatch(BaseModel):
    date: datetime
    season: int
    league: str
    team1: str
    team2: str
    spi1: float
    spi2: float
    prob1: float
    prob2: float
    probtie: float
    proj_score1: int
    proj_score2: int
    score1: int
    score2: int
