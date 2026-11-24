import type { ComponentChildren } from "preact";

interface IconProps {
  size?: number;
  cls?: string;
}

function Svg({ size = 15, cls, children }: IconProps & { children: ComponentChildren }) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      stroke-width="1.85"
      stroke-linecap="round"
      stroke-linejoin="round"
      class={cls}
      aria-hidden="true"
    >
      {children}
    </svg>
  );
}

export function BrandMark({ size = 24 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 32 32" class="brand-svg" aria-hidden="true">
      <rect width="32" height="32" rx="8" fill="currentColor" />
      <path
        d="M9 22V10l7 7 7-7v12"
        fill="none"
        stroke="var(--accent-ink)"
        stroke-width="2.6"
        stroke-linecap="round"
        stroke-linejoin="round"
      />
    </svg>
  );
}

export function IconPlus({ size }: IconProps) {
  return <Svg size={size}><path d="M12 5v14M5 12h14" /></Svg>;
}

export function IconSun({ size }: IconProps) {
  return (
    <Svg size={size}>
      <circle cx="12" cy="12" r="4" />
      <path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41" />
    </Svg>
  );
}

export function IconMoon({ size }: IconProps) {
  return <Svg size={size}><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" /></Svg>;
}

export function IconMonitor({ size }: IconProps) {
  return (
    <Svg size={size}>
      <rect x="2" y="3" width="20" height="14" rx="2" />
      <path d="M8 21h8M12 17v4" />
    </Svg>
  );
}

export function IconSettings({ size }: IconProps) {
  return (
    <Svg size={size}>
      <circle cx="12" cy="12" r="3" />
      <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 1 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 1 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 1 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 1 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z" />
    </Svg>
  );
}

export function IconSidebar({ size }: IconProps) {
  return (
    <Svg size={size}>
      <rect x="3" y="3" width="18" height="18" rx="2" />
      <path d="M9 3v18" />
    </Svg>
  );
}

export function IconSearch({ size }: IconProps) {
  return (
    <Svg size={size}>
      <circle cx="11" cy="11" r="8" />
      <path d="M21 21l-4.35-4.35" />
    </Svg>
  );
}

export function IconX({ size }: IconProps) {
  return <Svg size={size}><path d="M18 6L6 18M6 6l12 12" /></Svg>;
}

export function IconTrash({ size }: IconProps) {
  return (
    <Svg size={size}>
      <path d="M3 6h18M8 6V4h8v2M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6" />
    </Svg>
  );
}

export function IconCopy({ size }: IconProps) {
  return (
    <Svg size={size}>
      <rect x="9" y="9" width="13" height="13" rx="2" />
      <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
    </Svg>
  );
}

export function IconCheck({ size }: IconProps) {
  return <Svg size={size}><path d="M20 6L9 17l-5-5" /></Svg>;
}

export function IconChevron({ size = 14, cls }: IconProps) {
  return <Svg size={size} cls={cls}><path d="M9 18l6-6-6-6" /></Svg>;
}

export function IconPaperclip({ size }: IconProps) {
  return (
    <Svg size={size}>
      <path d="M21.44 11.05l-9.19 9.19a6 6 0 0 1-8.49-8.49l9.19-9.19a4 4 0 0 1 5.66 5.66l-9.2 9.19a2 2 0 0 1-2.83-2.83l8.49-8.48" />
    </Svg>
  );
}

export function IconArrowUp({ size }: IconProps) {
  return <Svg size={size}><path d="M12 19V5M5 12l7-7 7 7" /></Svg>;
}

export function IconArrowDown({ size }: IconProps) {
  return <Svg size={size}><path d="M12 5v14M19 12l-7 7-7-7" /></Svg>;
}

export function IconPlay({ size }: IconProps) {
  return <Svg size={size}><polygon points="6 4 20 12 6 20 6 4" /></Svg>;
}

export function IconPause({ size }: IconProps) {
  return (
    <Svg size={size}>
      <rect x="6" y="4" width="4" height="16" rx="1" />
      <rect x="14" y="4" width="4" height="16" rx="1" />
    </Svg>
  );
}

export function IconStop({ size }: IconProps) {
  return <Svg size={size}><rect x="5" y="5" width="14" height="14" rx="2" /></Svg>;
}

export function IconSpark({ size }: IconProps) {
  return (
    <Svg size={size}>
      <path d="M12 3l1.9 5.8L20 12l-6.1 3.2L12 21l-1.9-5.8L4 12l6.1-3.2L12 3z" />
    </Svg>
  );
}

export function IconTerminal({ size }: IconProps) {
  return (
    <Svg size={size}>
      <path d="M4 17l6-6-6-6M12 19h8" />
    </Svg>
  );
}

export function IconBolt({ size }: IconProps) {
  return <Svg size={size}><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z" /></Svg>;
}

export function IconAlert({ size }: IconProps) {
  return (
    <Svg size={size}>
      <circle cx="12" cy="12" r="10" />
      <path d="M12 8v4M12 16h.01" />
    </Svg>
  );
}

export function IconExternal({ size }: IconProps) {
  return (
    <Svg size={size}>
      <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6M15 3h6v6M10 14L21 3" />
    </Svg>
  );
}

export function IconEye({ size }: IconProps) {
  return (
    <Svg size={size}>
      <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z" />
      <circle cx="12" cy="12" r="3" />
    </Svg>
  );
}

export function IconEyeOff({ size }: IconProps) {
  return (
    <Svg size={size}>
      <path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19m-6.72-1.07a3 3 0 1 1-4.24-4.24M1 1l22 22" />
    </Svg>
  );
}

export function IconSliders({ size }: IconProps) {
  return (
    <Svg size={size}>
      <path d="M4 21v-7M4 10V3M12 21v-9M12 8V3M20 21v-5M20 12V3M1 14h6M9 8h6M17 16h6" />
    </Svg>
  );
}

export function IconPlug({ size }: IconProps) {
  return (
    <Svg size={size}>
      <path d="M12 22v-5M9 8V2M15 8V2M18 8v5a4 4 0 0 1-4 4h-4a4 4 0 0 1-4-4V8z" />
    </Svg>
  );
}

export function IconChart({ size }: IconProps) {
  return (
    <Svg size={size}>
      <path d="M18 20V10M12 20V4M6 20v-6" />
    </Svg>
  );
}

export function IconKey({ size }: IconProps) {
  return (
    <Svg size={size}>
      <path d="M21 2l-2 2m-7.61 7.61a5.5 5.5 0 1 1-7.778 7.778 5.5 5.5 0 0 1 7.777-7.777zm0 0L15.5 7.5m0 0l3 3L22 7l-3-3m-3.5 3.5L19 4" />
    </Svg>
  );
}

export function IconClock({ size }: IconProps) {
  return (
    <Svg size={size}>
      <circle cx="12" cy="12" r="10" />
      <path d="M12 6v6l4 2" />
    </Svg>
  );
}
