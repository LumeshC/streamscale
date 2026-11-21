// Small inline icons (no icon font / external assets).
type P = { size?: number; title?: string };

const svg = (size: number, title: string | undefined, children: React.ReactNode) => (
  <svg
    width={size}
    height={size}
    viewBox="0 0 16 16"
    fill="none"
    stroke="currentColor"
    strokeWidth={1.8}
    strokeLinecap="round"
    strokeLinejoin="round"
    aria-hidden={title ? undefined : true}
    role={title ? "img" : undefined}
  >
    {title ? <title>{title}</title> : null}
    {children}
  </svg>
);

export const CheckCircle = ({ size = 14, title }: P) =>
  svg(size, title, <><circle cx="8" cy="8" r="6.5" /><path d="M5.2 8.2l1.9 1.9 3.7-4" /></>);
export const AlertTriangle = ({ size = 14, title }: P) =>
  svg(size, title, <><path d="M8 2.2l6.2 11H1.8z" /><path d="M8 6.5v3" /><path d="M8 11.4v.1" /></>);
export const XCircle = ({ size = 14, title }: P) =>
  svg(size, title, <><circle cx="8" cy="8" r="6.5" /><path d="M5.8 5.8l4.4 4.4M10.2 5.8l-4.4 4.4" /></>);
export const MinusCircle = ({ size = 14, title }: P) =>
  svg(size, title, <><circle cx="8" cy="8" r="6.5" /><path d="M5.2 8h5.6" /></>);
export const Refresh = ({ size = 14, title }: P) =>
  svg(size, title, <><path d="M13.5 8a5.5 5.5 0 1 1-1.6-3.9" /><path d="M13.6 2.5v3.2h-3.2" /></>);
export const Sun = ({ size = 14, title }: P) =>
  svg(size, title, <><circle cx="8" cy="8" r="3" /><path d="M8 1.5v1.4M8 13.1v1.4M1.5 8h1.4M13.1 8h1.4M3.4 3.4l1 1M11.6 11.6l1 1M3.4 12.6l1-1M11.6 4.4l1-1" /></>);
export const Moon = ({ size = 14, title }: P) =>
  svg(size, title, <path d="M13.2 9.6A5.6 5.6 0 0 1 6.4 2.8a5.6 5.6 0 1 0 6.8 6.8z" />);
export const TableIcon = ({ size = 14, title }: P) =>
  svg(size, title, <><rect x="2" y="3" width="12" height="10" rx="1.5" /><path d="M2 6.5h12M2 10h12M6.5 6.5V13" /></>);
export const ChartIcon = ({ size = 14, title }: P) =>
  svg(size, title, <><path d="M2.5 13.5h11" /><path d="M3.5 10.5l3-3.5 2.5 2 4-5" /></>);
