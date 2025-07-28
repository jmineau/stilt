#' Get namelist from configuration
#' @author James Mineau
#'
#' Extracts a named list of parameters for STILT simulations from the configuration.
#'
#' @param config A list containing configuration parameters for STILT.
#' @return A named list containing the parameters required for STILT simulations.
#'
#' @export

get_namelist_from_config <- function(config) {

  if (is.null(config$winderrtf)) {
    config$winderrtf <- 0
  }

  # Aggregate STILT/HYSPLIT namelist
  namelist <- list(
    capemin = config$capemin,
    cmass = config$cmass,
    conage = config$conage,
    cpack = config$cpack,
    delt = config$delt,
    dxf = config$dxf,
    dyf = config$dyf,
    dzf = config$dzf,
    efile = config$efile,
    frhmax = config$frhmax,
    frhs = config$frhs,
    frme = config$frme,
    frmr = config$frmr,
    frts = config$frts,
    frvs = config$frvs,
    hscale = config$hscale,
    ichem = config$ichem,
    idsp = config$idsp,
    initd = config$initd,
    k10m = config$k10m,
    kagl = config$kagl,
    kbls = config$kbls,
    kblt = config$kblt,
    kdef = config$kdef,
    khinp = config$khinp,
    khmax = config$khmax,
    kmix0 = config$kmix0,
    kmixd = config$kmixd,
    kmsl = config$kmsl,
    kpuff = config$kpuff,
    krand = config$krand,
    krnd = config$krnd,
    kspl = config$kspl,
    kwet = config$kwet,
    kzmix = config$kzmix,
    maxdim = config$maxdim,
    maxpar = config$maxpar,
    mgmin = config$mgmin,
    mhrs = config$mhrs,
    nbptyp = config$nbptyp,
    ncycl = config$ncycl,
    ndump = config$ndump,
    ninit = config$ninit,
    nstr = config$nstr,
    nturb = config$nturb,
    numpar = config$numpar,
    nver = config$nver,
    outdt = config$outdt,
    p10f = config$p10f,
    pinbc = config$pinbc,
    pinpf = config$pinpf,
    poutf = config$poutf,
    qcycle = config$qcycle,
    rhb = config$rhb,
    rht = config$rht,
    splitf = config$splitf,
    tkerd = config$tkerd,
    tkern = config$tkern,
    tlfrac = config$tlfrac,
    tout = config$tout,
    tratio = config$tratio,
    tvmix = config$tvmix,
    varsiwant = config$varsiwant,
    veght = config$veght,
    vscale = config$vscale,
    vscaleu = config$vscaleu,
    vscales = config$vscales,
    wbbh = config$wbbh,
    wbwf = config$wbwf,
    wbwr = config$wbwr,
    winderrtf = config$winderrtf,
    wvert = config$wvert,
    zicontroltf = config$zicontroltf,
  )

  return(namelist)
}