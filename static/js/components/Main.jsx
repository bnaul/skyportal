// Baselayer components
import WebSocket from 'baselayer/components/WebSocket';
import { Notifications } from 'baselayer/components/Notifications';

// React and Redux
import React from 'react';
import PropTypes from 'prop-types';
import { Provider } from 'react-redux';
import ReactDOM from 'react-dom';

import { BrowserRouter, Link } from 'react-router-dom';
import { Switch } from 'react-router';
import PropsRoute from '../route';

// Main style
import styles from './Main.css';

// Store
import configureStore from '../store';

// Local
import CustomMessageHandler from '../CustomMessageHandler';
import CachedSource from '../containers/CachedSource';
import GroupContainer from '../containers/GroupContainer';
import SourceListContainer from '../containers/SourceListContainer';
import GroupListContainer from '../containers/GroupListContainer';
import NoMatchingRoute from './NoMatchingRoute';
import Profile from '../containers/Profile';
import Logo from '../containers/Logo';
import * as Action from '../actions';
import Responsive from '../components/Responsive';


const store = configureStore({});
const messageHandler = (
  new CustomMessageHandler(store.dispatch, store.getState)
);

class MainContent extends React.Component {
  async componentDidMount() {
    await store.dispatch(Action.hydrate());
    store.dispatch(Action.rotateLogo());
  }
  render() {
    return (
      <div className={styles.main}>

        <div className={styles.topBanner}>
          <div className={styles.topBannerContent}>
            <Logo className={styles.logo} />
            <Link className={styles.title} to="/">SkyPortal ∝</Link>
            <div className={styles.websocket}>
              <WebSocket
                url={`ws://${this.props.root}websocket`}
                auth_url={`${window.location.protocol}//${this.props.root}socket_auth_token`}
                messageHandler={messageHandler}
                dispatch={store.dispatch}
              />
            </div>
            <Responsive desktopElement={Profile} />
          </div>
        </div>

        <Responsive mobileElement={Profile} />

        <div className={styles.content}>

          <Notifications />

          <Switch>
            <PropsRoute exact path="/" component={SourceListContainer} />
            {'See https://stackoverflow.com/a/35604855 for syntax'}
            <PropsRoute path="/source/:id" component={CachedSource} />
            <PropsRoute exact path="/groups/" component={GroupListContainer} />
            <PropsRoute path="/group/:id" component={GroupContainer} />
            <PropsRoute component={NoMatchingRoute} />
          </Switch>

        </div>

        <div className={styles.footer}>
          <div className={styles.footerContent}>
            This is a first proof of concept. Please file issues at&nbsp;
            <a href="https://github.com/skyportal/skyportal">
              https://github.com/skyportal/skyportal
            </a>.
          </div>
        </div>

      </div>
    );
  }
}

MainContent.propTypes = {
  root: PropTypes.string.isRequired
};

ReactDOM.render(
  <Provider store={store}>
    <BrowserRouter basename="/">
      <MainContent root={`${window.location.host}/`} />
    </BrowserRouter>
  </Provider>,
  document.getElementById('content')
);
