// DisclaimerBanner — yellow notice that the data is automated and does not
// constitute a formal accusation.
//
// The disclaimer text comes from FichaCompleta.disclaimer so the backend
// controls the exact wording. This component is purely presentational.

interface DisclaimerBannerProps {
  message: string;
}

export function DisclaimerBanner({ message }: DisclaimerBannerProps) {
  return (
    <div
      role="note"
      className="rounded-md border border-yellow-300 bg-yellow-50 px-4 py-3 text-sm text-yellow-800"
    >
      <span className="font-semibold">Aviso: </span>
      {message}
    </div>
  );
}
