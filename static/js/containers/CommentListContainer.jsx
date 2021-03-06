import React from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';

import * as Action from '../actions';
import CommentList from '../components/CommentList';


const CommentListContainer = ({ source, addComment }) => (
  <CommentList
    comments={source.comments}
    source_id={source.id}
    addComment={addComment}
  />
);

CommentListContainer.propTypes = {
  source: PropTypes.shape({
    comments: PropTypes.arrayOf(PropTypes.object),
    source_id: PropTypes.string
  }).isRequired,
  addComment: PropTypes.func.isRequired
};

const mapStateToProps = (state, ownProps) => (
  {
    source: state.source
  }
);

const mapDispatchToProps = (dispatch, ownProps) => (
  {
    addComment: text => dispatch(
      Action.addComment({ source_id: ownProps.source, text })
    )
  }
);

export default connect(mapStateToProps, mapDispatchToProps)(CommentListContainer);
