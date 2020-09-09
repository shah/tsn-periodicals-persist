import * as pd from "@shah/tsn-periodicals";
import * as fs from "fs";
import * as ptab from "@shah/tsn-persist-tabular";

export interface RelationalCsvTableNames {
    readonly suppliers: string;
    readonly periodicals: string;
    readonly periodicalAnchors: string;
    readonly periodicalCommonAnchors: string;
    readonly periodicalEditions: string;
    readonly editionAnchors: string;
}

export interface RelationalCsvTableWriters {
    readonly names: RelationalCsvTableNames,
    readonly suppliers: ptab.TabularWriter<pd.PeriodicalSupplier>;
    readonly periodicals: ptab.TabularWriter<pd.Periodical>;
    readonly periodicalAnchors: ptab.TabularWriter<pd.PeriodicalAnchor>;
    readonly periodicalCommonAnchors: ptab.TabularWriter<pd.PeriodicalAnchor>;
    readonly periodicalEditions: ptab.TabularWriter<pd.PeriodicalEdition>;
    readonly editionAnchors: ptab.TabularWriter<pd.ClassifiedAnchor>;
    close(): void;
}

export class DefaultRelationalCsvTableWriters implements RelationalCsvTableWriters {
    static readonly UUID_NAMESPACE: ptab.UUID = "3438161e-47a2-415d-8fc8-ae8ed80a7c86";
    static readonly NAMES: RelationalCsvTableNames = {
        suppliers: "suppliers.csv",
        periodicals: "periodicals.csv",
        periodicalAnchors: "periodical-anchors.csv",
        periodicalCommonAnchors: "periodical-anchors-common.csv",
        periodicalEditions: "periodical-editions.csv",
        editionAnchors: "periodical-edition-anchors.csv"
    }

    readonly names: RelationalCsvTableNames;
    readonly suppliers: ptab.TabularWriter<pd.PeriodicalSupplier>;
    readonly periodicals: ptab.TabularWriter<pd.Periodical>;
    readonly periodicalAnchors: ptab.TabularWriter<pd.PeriodicalAnchor>;
    readonly periodicalCommonAnchors: ptab.TabularWriter<pd.PeriodicalAnchor>;
    readonly periodicalEditions: ptab.TabularWriter<pd.PeriodicalEdition>;
    readonly editionAnchors: ptab.TabularWriter<pd.ClassifiedAnchor>;

    constructor(destPath: string, uuidNamespace = DefaultRelationalCsvTableWriters.UUID_NAMESPACE, writers?: Partial<RelationalCsvTableWriters>) {
        this.names = writers?.names || DefaultRelationalCsvTableWriters.NAMES;
        this.suppliers = writers?.suppliers || new ptab.TabularWriter({ destPath, fileName: this.names.suppliers, parentUuidNamespace: uuidNamespace });
        this.periodicals = writers?.periodicals || new ptab.TabularWriter({ destPath, fileName: this.names.periodicals, parentUuidNamespace: uuidNamespace });
        this.periodicalAnchors = writers?.periodicalAnchors || new ptab.TabularWriter({ destPath, fileName: this.names.periodicalAnchors, parentUuidNamespace: uuidNamespace });
        this.periodicalCommonAnchors = writers?.periodicalCommonAnchors || new ptab.TabularWriter({ destPath, fileName: this.names.periodicalCommonAnchors, parentUuidNamespace: uuidNamespace });
        this.periodicalEditions = writers?.periodicalEditions || new ptab.TabularWriter({ destPath, fileName: this.names.periodicalEditions, parentUuidNamespace: uuidNamespace });
        this.editionAnchors = writers?.editionAnchors || new ptab.TabularWriter({ destPath, fileName: this.names.editionAnchors, parentUuidNamespace: uuidNamespace });
    }

    close(): void {
        this.editionAnchors.close();
        this.periodicalEditions.close();
        this.periodicalCommonAnchors.close();
        this.periodicalAnchors.close();
        this.periodicals.close();
        this.suppliers.close();
    }

    static recreateDir(destPath: string): void {
        fs.rmdirSync(destPath, { recursive: true })
        fs.mkdirSync(destPath, { recursive: true });
    }

    static mkDir(destPath: string): void {
        if (!fs.existsSync(destPath)) {
            fs.mkdirSync(destPath, { recursive: true });
        }
    }
}

export class PersistRelationalCSV {
    readonly stats = {
        written: {
            suppliers: 0,
            periodicals: 0,
            periodicalAnchors: 0,
            periodicalEditions: 0,
            editionAnchors: 0,
        }
    }

    constructor(readonly writers: RelationalCsvTableWriters) {
    }

    persistSuppliers(suppliers: pd.PeriodicalSuppliers): void {
        for (const supplier of Object.values(suppliers).sort((left, right) => { return left.name.localeCompare(right.name) })) {
            this.persistSupplier(supplier);
        }
    }

    persistSupplier(supplier: pd.PeriodicalSupplier): ptab.UUID | undefined {
        const suppliersPK = this.writers.suppliers.createId(supplier.name);
        if (this.writers.suppliers.write({
            persist: { id: suppliersPK, name: supplier.name, periodicals: Object.keys(supplier.periodicals).length },
            source: supplier
        })) {
            this.stats.written.suppliers++;
            // sort entries so that repeated runs create the same order (and diff'ing is easier)
            for (const p of Object.values(supplier.periodicals).sort((left, right) => { return left.name.localeCompare(right.name) })) {
                this.persistPeriodical(suppliersPK, p);
            }
            return suppliersPK;
        }
        return undefined;
    }

    persistPeriodical(suppliersPK: string, p: pd.Periodical): ptab.UUID | undefined {
        const periodicalsPK = this.writers.periodicals.createId(p.name);
        if (this.writers.periodicals.write({
            persist: { id: periodicalsPK, supplier_id: suppliersPK, name: p.name, editions: p.editions.length },
            source: p
        })) {
            this.stats.written.periodicals++;
            this.persistAnchors(suppliersPK, periodicalsPK, p);
            return periodicalsPK;
        }
        return undefined;
    }

    persistAnchors(suppliersPK: string, periodicalsPK: string, p: pd.Periodical): void {
        // sort entries by anchor text so that repeated runs create the same order (and diff'ing is easier)
        for (const cea of Object.values(p.classifiedAnchors).sort((left, right) => { return left.anchorText.localeCompare(right.anchorText); })) {
            const periodicalAnchorPK = this.writers.periodicalAnchors.createId(periodicalsPK + cea.anchorText + cea.classification);
            const record = {
                persist: {
                    id: periodicalAnchorPK,
                    periodical_id: periodicalsPK,
                    periodical_name: p.name,
                    anchor_text_classified: cea.anchorText,
                    anchors_count: cea.count,
                    editions_count: p.editions.length,
                    classification: cea.classification.classificationID,
                    common_anchor: pd.isPeriodicalCommonAnchor(cea) ? 1 : 0,
                }, source: cea
            };
            if (this.writers.periodicalAnchors.write(record)) {
                this.stats.written.periodicalAnchors++;
                if (pd.isPeriodicalCommonAnchor(cea)) {
                    delete record.persist.common_anchor;
                    this.writers.periodicalCommonAnchors.write(record);
                }
            }
        }

        // sort by oldest edition first so that the newest goes to the end (for future diff'ing ease)
        for (const pe of p.editions.sort((left, right) => { return right.date.valueOf() - left.date.valueOf(); })) {
            const periodicalEditionsPK = this.writers.periodicalEditions.createId(suppliersPK + pe.supplierContentId);
            if (this.writers.periodicalEditions.write({
                persist: {
                    id: periodicalEditionsPK,
                    periodical_id: periodicalsPK,
                    supplier_content_id: pe.supplierContentId,
                    periodical_name: p.name,
                    from_address: pe.fromAddress,
                    from_name: pe.fromName,
                    date: pe.date.toISOString(),
                    anchors: pe.anchors.length,
                }, source: pe
            })) {
                this.stats.written.periodicalEditions++;
                pe.anchors.sort((left, right) => {
                    const ctCompare = left.classifierText.localeCompare(right.classifierText);
                    return ctCompare == 0 ? (left.href.localeCompare(right.href)) : ctCompare;
                }).forEach((ca) => {
                    const peAnchorsPK = this.writers.editionAnchors.createId(periodicalEditionsPK + ca.classifierText + ca.href);
                    if (this.writers.editionAnchors.write({
                        persist: {
                            id: peAnchorsPK,
                            edition_id: periodicalEditionsPK,
                            periodical_name: p.name,
                            date: pe.date.toISOString(),
                            classification: ca.classification.classificationID,
                            anchor_text_classified: ca.classifierText,
                            common_anchor: ca.classifiedBy ? (pd.isPeriodicalCommonAnchor(ca.classifiedBy) ? 1 : 0) : 0,
                            href: ca.href,
                        }, source: ca
                    })) {
                        this.stats.written.editionAnchors++;
                    }
                });
            }
        }
    }
}
