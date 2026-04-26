/**
 * i18n helper — lightweight client-side translation.
 *
 * Usage:
 *   import { t, setLocale } from '../i18n';
 *   t('nav.dashboard')  // "Dashboard" (en) or "Dashboard" (de)
 *   setLocale('de');     // switch to German
 */

import en from './en.json';
import de from './de.json';

type Translations = Record<string, unknown>;

const locales: Record<string, Translations> = { en, de };

let currentLocale = 'en';

export function setLocale(locale: string): void {
  if (locales[locale]) {
    currentLocale = locale;
    if (typeof localStorage !== 'undefined') {
      localStorage.setItem('amplifi_locale', locale);
    }
  }
}

export function getLocale(): string {
  if (typeof localStorage !== 'undefined') {
    return localStorage.getItem('amplifi_locale') || currentLocale;
  }
  return currentLocale;
}

export function t(key: string): string {
  const locale = getLocale();
  const translations = locales[locale] || locales.en;
  const parts = key.split('.');
  let current: unknown = translations;
  for (const part of parts) {
    if (current && typeof current === 'object' && part in (current as Record<string, unknown>)) {
      current = (current as Record<string, unknown>)[part];
    } else {
      // Fallback to English
      let fallback: unknown = locales.en;
      for (const p of parts) {
        if (fallback && typeof fallback === 'object' && p in (fallback as Record<string, unknown>)) {
          fallback = (fallback as Record<string, unknown>)[p];
        } else {
          return key;
        }
      }
      return typeof fallback === 'string' ? fallback : key;
    }
  }
  return typeof current === 'string' ? current : key;
}

export function availableLocales(): string[] {
  return Object.keys(locales);
}
