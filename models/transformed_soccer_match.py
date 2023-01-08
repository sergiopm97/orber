from pydantic import BaseModel


class TransformedSoccerMatch(BaseModel):
    spi1: float
    spi2: float
    prob1: float
    probtie: float
    prob2: float
    proj_score1: int
    proj_score2: int
    team1_score1_mean_last_6: float
    team1_score2_mean_last_6: float
    team2_score2_mean_last_6: float
    team2_score1_mean_last_6: float
    score1: int
    score2: int
    winner: int
    goals: int
    over2: int
    btts: int
