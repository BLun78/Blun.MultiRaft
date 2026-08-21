import { ApplicationConfig, provideBrowserGlobalErrorListeners } from '@angular/core';

// No router: the observer is one screen. Everything on it is live, so there is nothing to navigate to.
export const appConfig: ApplicationConfig = {
  providers: [provideBrowserGlobalErrorListeners()],
};
