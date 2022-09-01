from dataclasses import dataclass

@dataclass(frozen=True)
class Venue:
    __slots__ = ['mlbam','name']
    mlbam: int
    name: str
    
    def __str__(self):
        return self.name

    def __repr__(self):
        return f'<{self.name}: {self.mlbam}>'
    
    @property
    def id(self):
        return self.mlbam

