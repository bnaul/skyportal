import numpy as np

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as psql
from sqlalchemy.orm import backref, relationship

from baselayer.app.models import (init_db, join_model, Base, DBSession, ACL,
                                  Role, User)


def is_owned_by(self, user):
    if hasattr(self, 'users'):
        return (user in self.users)
    elif hasattr(self, 'groups'):
        return bool(set(self.groups) & set(user.groups))
    else:
        raise NotImplementedError(f"{type(self).__name__} object has no owner")
Base.is_owned_by = is_owned_by


class NumpyArray(sa.types.TypeDecorator):
    impl = psql.ARRAY(sa.Float)

    def __repr__(self):
        """Override for correct `alembic` parsing."""
        return 'NumpyArray()'

    def process_result_value(self, value, dialect):
        return np.array(value)


class Group(Base):
    name = sa.Column(sa.String, unique=True, nullable=False)
    sources = relationship('Source', secondary='group_sources', cascade='all')
    streams = relationship('Stream', secondary='stream_groups', cascade='all',
                           back_populates='groups')
    group_users = relationship('GroupUser', back_populates='group',
                               cascade='all', passive_deletes=True)
    users = relationship('User', secondary='group_users', cascade='all',
                         back_populates='groups')


GroupUser = join_model('group_users', Group, User)
GroupUser.admin = sa.Column(sa.Boolean, nullable=False, default=False)


class Stream(Base):
    name = sa.Column(sa.String, unique=True, nullable=False)
    url = sa.Column(sa.String, unique=True, nullable=False)
    username = sa.Column(sa.String)
    password = sa.Column(sa.String)
    groups = relationship('Group', secondary='stream_groups', cascade='all',
                          back_populates='streams')


StreamGroup = join_model('stream_groups', Stream, Group)


User.group_users = relationship('GroupUser', back_populates='user', cascade='all')
User.groups = relationship('Group', secondary='group_users', cascade='all',
                           back_populates='users')


class Source(Base):
    id = sa.Column(sa.String, primary_key=True)
    ra = sa.Column(sa.Float)
    dec = sa.Column(sa.Float)
    red_shift = sa.Column(sa.Float, nullable=True)
    groups = relationship('Group', secondary='group_sources', cascade='all')
    comments = relationship('Comment', back_populates='source', cascade='all')
    photometry = relationship('Photometry', back_populates='source', cascade='all')
    spectra = relationship('Spectrum', back_populates='source', cascade='all')


GroupSource = join_model('group_sources', Group, Source)
User.sources = relationship('Source', secondary='join(Group, group_sources).join(group_users)',
                            primaryjoin='group_users.c.user_id == users.c.id')


class Telescope(Base):
    name = sa.Column(sa.String, nullable=False)
    nickname = sa.Column(sa.String, nullable=False)
    lat = sa.Column(sa.Float, nullable=False)
    lon = sa.Column(sa.Float, nullable=False)
    elevation = sa.Column(sa.Float, nullable=False)
    diameter = sa.Column(sa.Float, nullable=False)
    instruments = relationship('Instrument', back_populates='telescope',
                               cascade='all')


class Instrument(Base):
    telescope_id = sa.Column(sa.ForeignKey('telescopes.id',
                                           ondelete='CASCADE'),
                             nullable=False, index=True)
    telescope = relationship('Telescope', back_populates='instruments',
                             cascade='all')
    name = sa.Column(sa.String, nullable=False)
    type = sa.Column(sa.String, nullable=False)
    band = sa.Column(sa.String, nullable=False)
    photometry = relationship('Photometry', back_populates='instrument',
                              cascade='all')
    spectra = relationship('Spectrum', back_populates='instrument',
                           cascade='all')


class Comment(Base):
    user_id = sa.Column(sa.ForeignKey('users.id', ondelete='CASCADE'),
                        nullable=False, index=True)
    user = relationship('User', back_populates='comments', cascade='all')
    text = sa.Column(sa.String, nullable=False)
    source_id = sa.Column(sa.ForeignKey('sources.id', ondelete='CASCADE'),
                          nullable=False, index=True)
    source = relationship('Source', back_populates='comments', cascade='all')
    attachment_name = sa.Column(sa.String, nullable=True)
    attachment_type = sa.Column(sa.String, nullable=True)
    attachment_bytes = sa.Column(sa.types.LargeBinary, nullable=True)


User.comments = relationship('Comment', back_populates='user', cascade='all')


class Photometry(Base):
    __tablename__ = 'photometry'
    source_id = sa.Column(sa.ForeignKey('sources.id', ondelete='CASCADE'),
                          nullable=False, index=True)
    source = relationship('Source', back_populates='photometry', cascade='all')
    instrument_id = sa.Column(sa.ForeignKey('instruments.id',
                                            ondelete='CASCADE'),
                              nullable=False, index=True)
    instrument = relationship('Instrument', back_populates='photometry',
                              cascade='all')
    obs_time = sa.Column(sa.DateTime)
    mag = sa.Column(sa.Float)
    e_mag = sa.Column(sa.Float)
    lim_mag = sa.Column(sa.Float)
    filter = sa.Column(sa.String)  # TODO -> filters table?


class Spectrum(Base):
    __tablename__ = 'spectra'
    source_id = sa.Column(sa.ForeignKey('sources.id', ondelete='CASCADE'),
                          nullable=False, index=True)
    source = relationship('Source', back_populates='spectra', cascade='all')
    observed_at = sa.Column(sa.DateTime, nullable=False)
    # TODO program?
    instrument_id = sa.Column(sa.ForeignKey('instruments.id',
                                            ondelete='CASCADE'),
                              nullable=False, index=True)
    instrument = relationship('Instrument', back_populates='spectra',
                              cascade='all')
    # TODO better numpy integration
    wavelengths = sa.Column(NumpyArray, nullable=False)
    fluxes = sa.Column(NumpyArray, nullable=False)
    errors = sa.Column(NumpyArray)
